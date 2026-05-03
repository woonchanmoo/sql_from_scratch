from config.tools.validation_tools import *
from config.tools.basic_tools import *
from config.messages.messages import *

# query_tools는 실제 쿼리 수행 로직을 담고 있다.
# 각 함수는 validation을 먼저 수행한 뒤 트랜잭션에서 메타/데이터 조작을 수행한다.

def create_table(txn, schema):
    """Validate schema and create a new table metadata entry."""
    try:
        validate_create(txn, schema)
        
        create_schema(txn, schema)

        table_name = schema.get("table_name")
        add_tables(txn, table_name)

        return ExecutionResult(
            result=Result("CreateTableSuccess", schema["table_name"])
        )

    except Exception as e:
        return ExecutionResult(error=e)

def drop_table(txn, table_name):
    
    try:
        # 1. validation
        validate_drop(txn, table_name)

        # 2. schema 삭제
        schema_key = f"meta:table:{table_name}:schema".encode()
        txn.delete(schema_key)

        # 3. data 삭제
        prefix = f"data:{table_name}:".encode()
        cursor = txn.cursor()

        for key, _ in cursor:
            if key.startswith(prefix):
                txn.delete(key)

        # 4. table list에서도 제거
        remove_table(txn, table_name)

        return ExecutionResult(
            result=Result("DropSuccess", table_name)
        )
    
    except Exception as e:
        return ExecutionResult(error=e)
    
def explain_table(txn, table_name, commandType):
    """Print table schema details for EXPLAIN / DESCRIBE / DESC commands."""
    try:
        # 1. Validation
        if commandType == "explain":
            validate_explain(txn, table_name)
        elif commandType == "describe":
            validate_describe(txn, table_name)
        elif commandType == "desc":
            validate_desc(txn, table_name)

        # 2. table의 schema를 가져오기
        schema = get_schema(txn, table_name)

        # 3. schema의 내용을 표로 출력
        print("-" * 65)
        print(f"{'column_name':<20} | {'type':<11} | {'null':<5} | {'key':<10}")
        
        column_names = schema.get("column_names", [])
        columns = schema.get("columns", {})

        # Primary Key 추출
        primary_keys = set()
        primary_keys_list = schema.get("primary_keys", [])
        for pk_group in primary_keys_list:
            for col in pk_group:  # 내부 리스트를 한 번 더 순회하여 컬럼명을 꺼냅니다.
                primary_keys.add(col)
                
        foreign_keys = set()
        for fk in schema.get("foreign_keys", []):
            for col in fk.get("columns", []):
                foreign_keys.add(col)

        for col_name in column_names:
            col_info = columns.get(col_name)
            
            # 타입 포맷팅 (int or char(n))
            col_type = col_info["type"]
            if isinstance(col_type, dict):
                type_str = f"{col_type['base']}({col_type['length']})"
            else:
                type_str = col_type

            # Null 여부 (not_null: True -> 'N', False -> 'Y')
            null_str = "N" if col_info.get("not_null") else "Y"

            # Key 여부 판별 (PRI, FOR, PRI/FOR)
            is_pk = col_name in primary_keys
            is_fk = col_name in foreign_keys
            
            key_str = ""
            if is_pk and is_fk:
                key_str = "PRI/FOR"
            elif is_pk:
                key_str = "PRI"
            elif is_fk:
                key_str = "FOR"

            print(f"{col_name:<20} | {type_str:<11} | {null_str:<5} | {key_str:<10}")

        print("-" * 65)

        # row 개수 출력
        count = len(column_names)
        if count == 1:
            print(f"{count} row in set")
        else:
            print(f"{count} rows in set")

        # 4. 결과 반환 (건수 출력은 호출부나 Result 객체 내부 정의에 따라 조절)
        return ExecutionResult(
            result=Result("ExplainSuccess", None)
        )
    
    except Exception as e:
        return ExecutionResult(error=e)
    
def show_tables(txn):
    """Print the list of tables currently registered in the database."""
    try:
        tables = get_tables(txn)
        print("-" * 65)
        if tables:
            for table in tables:
                print(table)
        print("-" * 65)
        # 5. Row 개수 출력
        count = len(tables)
        if count == 1:
            print(f"{count} row in set")
        else:
            print(f"{count} rows in set")

        return ExecutionResult(
            result=Result("ShowTablesSuccess", None)
        )

    except Exception as e:
        return ExecutionResult(error=e) 
    
def insert_into_table(txn, insert_schema):
    """Insert a row into a table, mapping given values to target columns."""
    try:
        table_name = insert_schema.get("table_name", "")
        # 사용자가 명시한 컬럼 리스트 (없으면 None)
        input_columns = insert_schema.get("column_names") 
        values = insert_schema.get("values", [])

        # 1. 테이블 스키마 가져오기
        schema = get_schema(txn, table_name)
        all_column_names = schema.get("column_names", [])
        columns_info = schema.get("columns", {})

        # 2. 삽입될 컬럼 순서 결정 및 매핑 딕셔너리 생성
        # 예: {"col1": "val1", "col2": "val2"}
        target_columns = input_columns if input_columns else all_column_names
        
        # 입력된 컬럼-값 쌍을 매핑 (나중에 찾기 쉽게)
        input_data_map = dict(zip(target_columns, values))

        # 3. 전체 컬럼 순서에 맞춰 데이터 준비 (Truncate 포함)
        processed_values = []
        for col_name in all_column_names:
            col_meta = columns_info.get(col_name)
            col_type = col_meta.get("type")
            
            # 해당 컬럼에 대해 입력된 값이 있는지 확인
            if col_name in input_data_map:
                val = input_data_map[col_name]
                
                # char 타입 truncate 처리
                if isinstance(col_type, dict) and col_type.get("base") == "char":
                    max_len = col_type.get("length")
                    if isinstance(val, str) and len(val) > max_len:
                        val = val[:max_len]
                
                processed_values.append(val)
            else:
                # 값이 명시되지 않은 컬럼은 NULL(None) 삽입
                processed_values.append(None)

        # 4. Validation (테이블 존재 여부 등)
        validate_insert(txn, table_name, input_columns, values, all_column_names, processed_values, columns_info)

        # 5. Row 삽입
        add_row(txn, table_name, processed_values)

        return ExecutionResult(
            result=Result("InsertResult", "success")
        )

    except Exception as e:
        return ExecutionResult(error=e)
    
def select_table(txn, select_schema):
    """Validate and display rows for a SELECT query from one or more tables."""
    try:
        table_names = select_schema["from_list"]

        # 1. Validation
        for table_name in table_names:
            validate_select(txn, table_name)
        
        target_table = table_names[0]
        schema = get_schema(txn, target_table)
        
        # 2. 컬럼 헤더 준비
        all_columns = schema.get("column_names", [])
        col_width = 20  # 각 컬럼의 고정 너비 설정
        
        # 3. 데이터 로드
        rows = get_rows(txn, target_table)
        count = len(rows)
        
        # 4. 출력 포맷팅
        # 구분선 생성 (컬럼 개수만큼 '-' 반복)
        line_length = (col_width + 3) * len(all_columns)
        print("-" * line_length)
        
        if count > 0:
            # 헤더 출력 (대문자 정렬)
            header_str = " | ".join([f"{col.upper():<{col_width}}" for col in all_columns])
            print(header_str)
            
            # 데이터 행 출력
            for row in rows:
                # 각 컬럼 값을 문자열로 변환하되, None이면 'null' 출력 + 정렬 처리
                row_items = []
                for val in row:
                    display_val = str(val) if val is not None else "null"
                    row_items.append(f"{display_val:<{col_width}}")
                
                print(" | ".join(row_items))
        
        print("-" * line_length)
        
        # 5. Row 개수 출력
        if count == 1:
            print(f"{count} row in set")
        else:
            print(f"{count} rows in set")

        return ExecutionResult(
            result=Result("SelectSuccess", None)
        )

    except Exception as e:
        # validate_select 등에서 발생한 에러 처리 (SelectTableExistenceError 포함)
        return ExecutionResult(error=e)
    
def rename_table(txn, rename_schema):
    """Rename a table and update related metadata, foreign keys, counters, and data keys."""
    try:
        old_name = rename_schema.get("old_name")
        new_name = rename_schema.get("new_name")

        # 1. Validation (기존 테이블 존재 여부, 새 이름 중복 여부 등)
        validate_rename(txn, old_name, new_name)

        # 2. FK를 업데이트
        update_foreign_keys(txn, old_name, new_name)

        # 3. Schema 변경
        schema = get_schema(txn, old_name)
        schema["table_name"] = new_name  # 스키마 내부의 테이블 이름 필드 업데이트
        create_schema(txn, schema)       # 새 키(meta:table:new_name:schema)로 저장
        delete_schema(txn, old_name)     # 기존 키 삭제

        update_foreign_keys(txn, old_name, new_name)

        # 4. Counter 변경
        rename_counter(txn, old_name, new_name)

        # 5. Data (Rows) 변경
        rename_data_rows(txn, old_name, new_name)

        # 6. Global Table List 업데이트
        remove_table(txn, old_name)
        add_tables(txn, new_name)

        return ExecutionResult(
            result=Result("RenameSuccess", new_name)
        )

    except Exception as e:
        return ExecutionResult(error=e)
    
def truncate_table(txn, table_name):
    """Remove all rows from a table and reset its counter."""
    
    try:
        # 1. validation
        validate_truncate(txn, table_name)

        # 2. 데이터 행(Rows) 삭제
        delete_all_rows(txn, table_name)

        # 3. Counter 초기화
        reset_counter(txn, table_name)

        return ExecutionResult(
            result=Result("TruncateSuccess", table_name)
        )
    
    except Exception as e:
        return ExecutionResult(error=e)
    
def delete_from_table(txn, delete_schema):
    """Remove all rows from a table and reset its counter."""
    
    # try:
        # # 1. validation
        # validate_delete(txn, table_name)

        # # 2. 데이터 행(Rows) 삭제
        # delete_all_rows(txn, table_name)

        # # 3. Counter 초기화
        # reset_counter(txn, table_name)

        # return ExecutionResult(
        #     result=Result("TruncateSuccess", table_name)
        # )
    
    # except Exception as e:
    #     return ExecutionResult(error=e)