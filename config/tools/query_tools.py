from config.tools.validation_tools import *
from config.tools.basic_tools import *
from config.messages.messages import *
from config.messages.errors import *

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

        tables = get_tables(txn)
        if not table_name in tables:
            raise NoSuchTable("Insert into")

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

# ──────────────────────────────────────────────
# DELETE 함수
# ──────────────────────────────────────────────

def delete_from_table(txn, delete_schema):
    """
    DELETE FROM table [WHERE clause] 실행

    delete_schema 구조:
        {
            "table_name": str,
            "where_clause": dict or None   # None이면 WHERE 절 없음
        }

    처리 순서:
    1. 테이블 존재 확인 및 WHERE 절 정적 검증 (validate_delete)
    2. 테이블 스키마 및 전체 레코드 ID 로드
    3. WHERE 조건에 부합하는 레코드 ID 수집 (_collect_matching_records)
    4. FK 참조 무결성 검사 (_check_referential_integrity)
       - 위반 시 DeleteReferentialIntegrityPassed(count) raise → 아무것도 삭제하지 않음
    5. 위반 없으면 수집된 레코드를 삭제 후 DeleteResult(count) 반환
    """

    try:
        table_name = delete_schema.get("table_name")
        where_clause = delete_schema.get("where_clause")

        # 1. Validation: 테이블 존재 및 WHERE 절 컬럼/타입 정적 검증
        validate_delete(txn, table_name, where_clause)

        # 2. 스키마와 전체 레코드 ID 로드
        schema = get_schema(txn, table_name)
        all_record_ids = get_all_record_ids(txn, table_name)

        # 3. WHERE 조건을 만족하는 레코드 ID 목록 수집
        #    WHERE 절이 없으면 전체 레코드 반환
        records_to_delete = _collect_matching_records(
            txn, table_name, all_record_ids, where_clause, schema
        )

        # 4. FK 무결성 검사
        #    삭제 대상 중 하나라도 다른 테이블 FK에 의해 참조되면
        #    DeleteReferentialIntegrityPassed(len(records_to_delete)) raise
        _check_referential_integrity(txn, table_name, records_to_delete, schema)

        # 5. 삭제 수행
        for record_id in records_to_delete:
            delete_row(txn, table_name, record_id)

        # 6. 성공 결과 반환
        return ExecutionResult(
            result=Result("DeleteResult", len(records_to_delete))
        )

    except Exception as e:
        return ExecutionResult(error=e)

# ──────────────────────────────────────────────
# DELETE 내부 헬퍼 함수
# ──────────────────────────────────────────────

def _collect_matching_records(txn, table_name, all_record_ids, where_clause, schema):
    """
    WHERE 조건을 평가하여 삭제 대상 레코드 ID 리스트를 반환한다.

    - where_clause가 None이면 전체 레코드를 삭제 대상으로 반환한다.
    - where_clause가 있으면 각 레코드를 row_dict로 변환하고
      _evaluate_where 를 호출하여 조건을 평가한다.

    Args:
        txn             : LMDB transaction
        table_name      : 대상 테이블명
        all_record_ids  : 전체 레코드 ID 리스트 (get_all_record_ids 결과)
        where_clause    : WHERE 절 AST dict 또는 None
        schema          : 테이블 스키마 (column_names 등)

    Returns:
        list[int]: 조건을 만족하는 레코드 ID 리스트
    """
    # WHERE 절 없음 → 전체 삭제
    if where_clause is None:
        return list(all_record_ids)

    column_names = schema["column_names"]
    matching = []

    for record_id in all_record_ids:
        row_values = get_row(txn, table_name, record_id)
        if row_values is None:
            continue
        row_dict = dict(zip(column_names, row_values))
        if _evaluate_where(row_dict, where_clause, table_name):
            matching.append(record_id)

    return matching


def _evaluate_where(row_dict, where_clause, table_name):
    """
    WHERE 절 AST를 재귀적으로 평가하여 해당 행이 조건을 만족하는지 반환한다.

    지원하는 노드 타입:
        "and"            : operands 모두 True여야 True
        "or"             : operands 중 하나라도 True면 True
        "comparison"     : 두 operand를 비교 연산자로 비교
        "null_predicate" : IS NULL / IS NOT NULL 평가

    Args:
        row_dict    : {column_name: value} 형태의 행 데이터
        where_clause: WHERE 절 AST 노드
        table_name  : 대상 테이블명 (TableNotSpecified 검사 전달용)

    Returns:
        bool: 조건 만족 여부
    """
    t = where_clause["type"]

    if t == "and":
        return all(_evaluate_where(row_dict, op, table_name) for op in where_clause["operands"])
    if t == "or":
        return any(_evaluate_where(row_dict, op, table_name) for op in where_clause["operands"])
    if t == "comparison":
        return _evaluate_comparison(row_dict, where_clause, table_name)
    if t == "null_predicate":
        return _evaluate_null_predicate(row_dict, where_clause)
    return False


def _evaluate_comparison(row_dict, comparison, table_name):
    """
    comparison 노드를 평가하여 비교 결과를 반환한다.

    comparison AST 구조:
        {
            "type"    : "comparison",
            "operator": "=" | "!=" | "<" | ">" | "<=" | ">=",
            "left"    : operand_node,
            "right"   : operand_node
        }

    operand_node는 다음 중 하나:
        {"type": "column", "table": str|None, "column": str}
        {"type": "value",  "value": int|str|None}

    타입 호환 규칙 (Table 1):
        char     : =, != 만 허용 → 그 외는 IncomparableError
        int/date : =, !=, <, >, <=, >= 모두 허용
        null 값  : null_predicate에서만 처리; comparison에서 null이 나오면 False 반환

    Args:
        row_dict   : {column_name: value} 형태의 행 데이터
        comparison : comparison AST 노드
        table_name : 대상 테이블명

    Returns:
        bool: 비교 결과

    Raises:
        IncomparableError: 허용되지 않는 타입 조합 또는 연산자 사용 시
    """
    lv = _resolve_operand(row_dict, comparison["left"],  table_name)
    rv = _resolve_operand(row_dict, comparison["right"], table_name)
    op = comparison["operator"]

    # null 값이 comparison에 나타나면 항상 False (IS NULL/IS NOT NULL을 사용해야 함)
    if lv is None or rv is None:
        return False

    # 런타임 타입 불일치는 validate_where_columns 에서 걸러졌어야 하지만 안전장치로 유지
    if type(lv) != type(rv):
        raise IncomparableError()

    if op == "=":   return lv == rv
    if op == "!=":  return lv != rv
    if op == "<":   return lv < rv
    if op == ">":   return lv > rv
    if op == "<=":  return lv <= rv
    if op == ">=":  return lv >= rv
    return False


def _evaluate_null_predicate(row_dict, null_predicate):
    """
    null_predicate 노드를 평가하여 IS NULL / IS NOT NULL 결과를 반환한다.

    null_predicate AST 구조:
        {
            "type"      : "null_predicate",
            "column"    : {"type": "column", "table": str|None, "column": str},
            "is_not_null": bool   # True → IS NOT NULL, False → IS NULL
        }

    Args:
        row_dict       : {column_name: value} 형태의 행 데이터
        null_predicate : null_predicate AST 노드

    Returns:
        bool: 평가 결과
    """
    col_name = null_predicate["column"]["column"]
    val = row_dict[col_name]
    if null_predicate["is_not_null"]:
        return val is not None      # IS NOT NULL
    else:
        return val is None          # IS NULL


def _resolve_operand(row_dict, operand, table_name):
    """
    operand 노드를 실제 Python 값으로 변환한다.

    - "value" 타입 : operand["value"] 를 그대로 반환
    - "column" 타입: row_dict에서 컬럼 값 조회
        * table prefix가 명시되어 있고 table_name과 다르면 TableNotSpecified raise

    Returns:
        int | str | None: 실제 값

    Raises:
        TableNotSpecified: 명시된 테이블이 FROM 절 테이블과 다를 때
    """
    if operand["type"] == "value":
        return operand["value"]

    # "column" 타입
    if operand["table"] is not None and operand["table"] != table_name:
        raise TableNotSpecified("where")
    return row_dict[operand["column"]]


def _check_referential_integrity(txn, table_name, record_ids, schema):
    """
    삭제 대상 레코드들이 다른 테이블의 FK에 의해 참조되는지 확인한다.

    확인 절차:
    1. 모든 테이블 스키마를 순회하며 ref_table == table_name 인 FK를 찾는다.
    2. 해당 FK를 가진 테이블의 모든 레코드를 순회한다.
    3. FK 컬럼 값 조합이 삭제 대상 레코드의 참조 컬럼 값 조합과 일치하면 위반으로 판정.
    4. 위반이 하나라도 있으면 DeleteReferentialIntegrityPassed(len(record_ids)) raise.
       → 이 때 count는 FK 위반 레코드 수가 아니라 삭제 요청된 전체 레코드 수이다.

    Args:
        txn        : LMDB transaction
        table_name : 삭제 대상 테이블명
        record_ids : 삭제 예정 레코드 ID 리스트
        schema     : 삭제 대상 테이블의 스키마

    Raises:
        DeleteReferentialIntegrityPassed: FK 위반 존재 시
    """
    if not record_ids:
        return

    all_tables = get_tables(txn)
    column_names = schema["column_names"]

    # 삭제 대상 레코드들의 참조 컬럼 값 집합을 미리 수집해 두면
    # 다른 테이블 레코드와의 대조가 빠르다.
    # FK마다 ref_columns가 다르므로 FK 단위로 검사한다.

    for other_table in all_tables:
        other_schema = get_schema(txn, other_table)
        if not other_schema:
            continue

        for fk in other_schema.get("foreign_keys", []):
            if fk["ref_table"] != table_name:
                continue

            ref_cols = fk["ref_columns"]    # 우리 테이블에서 참조당하는 컬럼들
            fk_cols  = fk["columns"]        # other_table에서 참조하는 컬럼들

            # 삭제 대상 레코드들의 ref_col 값 튜플 집합 구성
            target_value_sets = set()
            for rid in record_ids:
                row_vals = get_row(txn, table_name, rid)
                if row_vals is None:
                    continue
                row_dict  = dict(zip(column_names, row_vals))
                ref_tuple = tuple(row_dict.get(c) for c in ref_cols)
                # 참조 컬럼에 NULL이 있으면 FK 위반 대상이 아님
                if any(v is None for v in ref_tuple):
                    continue
                target_value_sets.add(ref_tuple)

            if not target_value_sets:
                continue

            # other_table의 각 레코드가 삭제 대상 값을 참조하는지 확인
            other_col_names = other_schema["column_names"]
            other_ids = get_all_record_ids(txn, other_table)

            for oid in other_ids:
                other_row = get_row(txn, other_table, oid)
                if other_row is None:
                    continue
                other_dict = dict(zip(other_col_names, other_row))
                fk_tuple   = tuple(other_dict.get(c) for c in fk_cols)

                if fk_tuple in target_value_sets:
                    # FK 위반: 삭제 요청된 전체 레코드 수를 count로 전달
                    raise DeleteReferentialIntegrityPassed(len(record_ids))

# ──────────────────────────────────────────────
# SELECT 함수
# ──────────────────────────────────────────────

def select_table(txn, select_schema):
    """
    SELECT 쿼리를 실행한다.

    select_schema 구조 (schema.py 참조):
        {
            "select_list" : [{"type":"star"} | {"type":"column","table":str|None,"column":str,"alias":str|None}
                              | {"type":"aggregate","func":str,"table":str|None,"column":str,"alias":str|None}],
            "from_list"   : [str, ...],          # 실제 테이블 이름 (alias 아님)
            "join_list"   : [{"_clause":"join","table":str,
                               "left":{"table":str|None,"column":str},
                               "right":{"table":str|None,"column":str}}, ...],
            "where_clause": dict | None,         # WHERE AST (DELETE와 동일 구조)
            "group_by"    : {"_clause":"group_by","table":str|None,"column":str} | None,
            "order_by"    : {"_clause":"order_by","table":str|None,"column":str,"direction":"asc"|"desc"} | None,
            "limit"       : int | None,
            "offset"      : int | None
        }

    처리 순서:
    1. FROM 테이블 스키마 로드
    2. 전체 Validation (validate_select_query)
    3. FROM + JOIN → 결합 행 생성  (row = {(table_name, col_name): value})
    4. WHERE 필터
    5a. GROUP BY → 그룹화 + 집계 투영
    5b. GROUP BY 없음 → 단순 컬럼 투영
    6. ORDER BY 정렬
    7. LIMIT / OFFSET 슬라이싱
    8. 결과 출력

    Note:
        from_list는 실제 테이블 이름만 담고 있어 alias(s, e 등)를 직접 해석할 수 없다.
        alias 지원이 필요하면 sql_transformer.py의 referred_table을 수정하여
        from_list를 [{"table": str, "alias": str}] 구조로 바꿔야 한다.
    """
    try:
        from_list    = select_schema["from_list"]
        join_list    = select_schema.get("join_list", [])
        where_clause = select_schema.get("where_clause")
        group_by     = select_schema.get("group_by")
        order_by     = select_schema.get("order_by")
        select_list  = select_schema["select_list"]
        limit        = select_schema.get("limit")
        offset       = select_schema.get("offset")

        # 1. FROM + JOIN 테이블 스키마 로드 (존재 확인 포함)
        all_table_names = list(from_list)
        for j in join_list:
            if j["table"] not in all_table_names:
                all_table_names.append(j["table"])
        schemas = _load_table_schemas(txn, all_table_names)

        # 2. 전체 Validation
        validate_select_query(txn, select_schema, schemas)

        # 3. FROM + JOIN → 결합 행 빌드
        #    row_dict = {(table_name, col_name): value, ...}
        rows = _build_joined_rows(txn, from_list, join_list, schemas)

        # 4. WHERE 필터
        if where_clause:
            rows = _filter_rows_where(rows, where_clause, schemas, from_list)

        # 5. GROUP BY 또는 단순 투영
        if group_by:
            rows, headers = _apply_group_by(rows, group_by, select_list, schemas)
        else:
            rows, headers = _project_columns(rows, select_list, schemas, from_list)

        # 6. ORDER BY
        if order_by:
            rows = _apply_order_by(rows, headers, order_by)

        # 7. LIMIT / OFFSET
        rows = _apply_limit_offset(rows, limit, offset)

        # 8. 출력
        _print_result(rows, headers)

        return ExecutionResult(result=Result("SelectSuccess", None))

    except Exception as e:
        return ExecutionResult(error=e)


# ──────────────────────────────────────────────
# SELECT 내부 헬퍼 함수
# ──────────────────────────────────────────────

def _load_table_schemas(txn, table_names):
    """FROM/JOIN 절의 모든 테이블 스키마를 로드한다."""
    schemas = {}
    for name in table_names:
        schema = get_schema(txn, name)
        if schema is None:
            raise SelectTableExistenceError(name)
        schemas[name] = schema
    return schemas


def _build_joined_rows(txn, from_list, join_list, schemas):
    """FROM + INNER JOIN으로 결합된 행 목록을 반환한다.
    row = {(table_name, col_name): value}
    """
    # Cartesian product of all FROM tables
    rows = [{}]
    for table in from_list:
        col_names = schemas[table]["column_names"]
        table_rows = get_rows(txn, table)
        new_rows = []
        for existing in rows:
            for vals in table_rows:
                merged = dict(existing)
                for col, val in zip(col_names, vals):
                    merged[(table, col)] = val
                new_rows.append(merged)
        rows = new_rows

    # INNER JOIN
    for jc in join_list:
        join_table = jc["table"]
        left_ref   = jc["left"]   # {"table": str|None, "column": str}
        right_ref  = jc["right"]

        col_names  = schemas[join_table]["column_names"]
        join_rows  = [
            {(join_table, c): v for c, v in zip(col_names, vals)}
            for vals in get_rows(txn, join_table)
        ]

        new_rows = []
        for existing in rows:
            lv = _resolve_value_in_row(existing, left_ref)
            for jr in join_rows:
                rv = _resolve_value_in_row(jr, right_ref)
                if lv is not None and lv == rv:
                    new_rows.append({**existing, **jr})
        rows = new_rows

    return rows


def _resolve_value_in_row(row, col_ref):
    """row = {(table,col): value} 에서 col_ref 값을 꺼낸다."""
    table = col_ref.get("table")
    col   = col_ref["column"]
    if table:
        return row.get((table, col))
    matches = [v for (_, c), v in row.items() if c == col]
    return matches[0] if len(matches) == 1 else None


def _filter_rows_where(rows, where_clause, schemas, from_list):
    """WHERE 조건을 만족하는 행만 반환한다."""
    return [row for row in rows if _evaluate_where_select(row, where_clause)]


def _evaluate_where_select(row, node):
    """WHERE AST를 재귀 평가한다. row = {(table,col): value}"""
    t = node["type"]
    if t == "and":
        return all(_evaluate_where_select(row, op) for op in node["operands"])
    if t == "or":
        return any(_evaluate_where_select(row, op) for op in node["operands"])
    if t == "comparison":
        lv = _resolve_value_in_row(row, node["left"])  if node["left"]["type"]  == "column" else node["left"]["value"]
        rv = _resolve_value_in_row(row, node["right"]) if node["right"]["type"] == "column" else node["right"]["value"]
        op = node["operator"]
        if lv is None or rv is None:
            return False
        if type(lv) != type(rv):
            return False
        if op == "=":  return lv == rv
        if op == "!=": return lv != rv
        if op == "<":  return lv <  rv
        if op == ">":  return lv >  rv
        if op == "<=": return lv <= rv
        if op == ">=": return lv >= rv
    if t == "null_predicate":
        val = _resolve_value_in_row(row, node["column"])
        return (val is not None) if node["is_not_null"] else (val is None)
    return False


def _project_columns(rows, select_list, schemas, from_list):
    """GROUP BY 없이 SELECT 컬럼 목록에 따라 행을 투영한다."""
    # ── STAR: all columns from all FROM tables in schema order
    if len(select_list) == 1 and select_list[0]["type"] == "star":
        keys    = [(t, c) for t in from_list for c in schemas[t]["column_names"]]
        headers = [c for _, c in keys]
        return [[row.get(k) for k in keys] for row in rows], headers

    # ── Specific columns (and/or global aggregates)
    headers = []
    for item in select_list:
        if item["type"] == "column":
            headers.append(item.get("alias") or item["column"])
        elif item["type"] == "aggregate":
            headers.append(item.get("alias") or f"{item['func']}({item['column']})")

    has_agg = any(item["type"] == "aggregate" for item in select_list)

    if has_agg:
        # Pre-compute each aggregate over all rows
        agg_vals = {}
        for i, item in enumerate(select_list):
            if item["type"] == "aggregate":
                values = [_resolve_value_in_row(row, {"table": item.get("table"), "column": item["column"]})
                          for row in rows]
                agg_vals[i] = _compute_aggregate(values, item["func"])
        # Single output row; use first row for plain columns (or None if empty)
        rep = rows[0] if rows else {}
        single = []
        for i, item in enumerate(select_list):
            if item["type"] == "aggregate":
                single.append(agg_vals[i])
            else:
                single.append(_resolve_value_in_row(rep, {"table": item.get("table"), "column": item["column"]}))
        return [single], headers

    # Plain columns only — project each row
    projected = []
    for row in rows:
        projected.append([
            _resolve_value_in_row(row, {"table": item.get("table"), "column": item["column"]})
            for item in select_list
        ])
    return projected, headers


def _apply_group_by(rows, group_by, select_list, schemas):
    """GROUP BY 기준으로 행을 묶고 집계 함수를 적용한다."""
    gb_ref = {"table": group_by.get("table"), "column": group_by["column"]}

    # 그룹화
    groups: dict = {}
    for row in rows:
        key = _resolve_value_in_row(row, gb_ref)
        groups.setdefault(key, []).append(row)

    # 헤더 결정
    headers = []
    for item in select_list:
        if item["type"] == "column":
            headers.append(item.get("alias") or item["column"])
        elif item["type"] == "aggregate":
            headers.append(item.get("alias") or f"{item['func']}({item['column']})")

    # 각 그룹 → 결과 행
    result_rows = []
    for group_rows in groups.values():
        proj = []
        for item in select_list:
            if item["type"] == "column":
                proj.append(_resolve_value_in_row(
                    group_rows[0],
                    {"table": item.get("table"), "column": item["column"]}
                ))
            elif item["type"] == "aggregate":
                values = [
                    _resolve_value_in_row(r, {"table": item.get("table"), "column": item["column"]})
                    for r in group_rows
                ]
                proj.append(_compute_aggregate(values, item["func"]))
        result_rows.append(proj)

    return result_rows, headers


def _compute_aggregate(values, func):
    """집계 함수(max/min/sum/avg)를 None 제외 값들에 적용한다."""
    non_null = [v for v in values if v is not None]
    if func == "max":
        return max(non_null) if non_null else None
    if func == "min":
        return min(non_null) if non_null else None
    if func == "sum":
        int_vals = [v for v in non_null if isinstance(v, int)]
        return sum(int_vals)          # 비어 있거나 비-int면 0
    if func == "avg":
        num_vals = [v for v in non_null if isinstance(v, (int, float))]
        return sum(num_vals) / len(num_vals) if num_vals else None
    return None


def _apply_order_by(rows, headers, order_by):
    """ORDER BY 절에 따라 rows를 정렬한다."""
    col_name  = order_by["column"]
    direction = order_by["direction"]   # "asc" | "desc"

    # headers에서 정렬 기준 인덱스 찾기 (alias / 컬럼명 모두 지원)
    try:
        idx = headers.index(col_name)
    except ValueError:
        return rows                     # 컬럼 없으면 정렬 생략 (validation에서 보장)

    reverse = direction == "desc"

    def sort_key(row):
        val = row[idx]
        # None을 가장 작은 값으로 취급: (0, ...) < (1, real_val)
        return (0, ) if val is None else (1, val)

    return sorted(rows, key=sort_key, reverse=reverse)


def _apply_limit_offset(rows, limit, offset):
    """LIMIT / OFFSET 절을 적용한다."""
    start = offset if offset is not None else 0
    if limit is not None:
        return rows[start: start + limit]
    return rows[start:]


def _print_result(rows, headers):
    """쿼리 결과를 테이블 형식으로 출력한다."""
    col_w = 15
    sep   = "-" * ((col_w + 3) * len(headers) - 1)

    print(sep)
    if headers:
        print(" | ".join(f"{h.upper():<{col_w}}" for h in headers))
        print(sep)
    for row in rows:
        print(" | ".join(f"{'null' if v is None else str(v):<{col_w}}" for v in row))
    print(sep)
    n = len(rows)
    print(f"{n} row{'s' if n != 1 else ''} in set")