import json

# 1. meta:tables
# 1.1. convert tables set into list and put it into meta:tables
def add_tables(txn, table_name: str):

    tables = get_tables(txn)
    tables.add(table_name)

    txn.put(b"meta:tables",
            json.dumps(sorted(list(tables))).encode())
    
def remove_table(txn, table_name: str):

    tables = get_tables(txn)
    tables.remove(table_name)

    txn.put(b"meta:tables",
            json.dumps(sorted(list(tables))).encode())

# 1.2. get the tables set from meta:tables
def get_tables(txn):
    tables_bytes = txn.get(b"meta:tables")
    if tables_bytes is None:
        return set()
    return set(json.loads(tables_bytes.decode()))

##############################################################################################################
# 2. meta:table:<table_name>:schema
# 2.1. create schema
def create_schema(txn, schema: dict):
    table_name = schema.get("table_name")
    key = f"meta:table:{table_name}:schema".encode()
    value = json.dumps(schema).encode()

    txn.put(key, value)

# 2.2. get schema
def get_schema(txn, table_name: str):
    key = f"meta:table:{table_name}:schema".encode()
    schema_bytes = txn.get(key)
    if schema_bytes is None:
        return None
    return json.loads(schema_bytes.decode())

# 2.3. delete schema
def delete_schema(txn, table_name: str):
    """기존 테이블의 스키마 메타데이터를 삭제"""
    key = f"meta:table:{table_name}:schema".encode()
    txn.delete(key)

# 2.4. update foreign keys
def update_foreign_keys(txn, old_name: str, new_name: str):
    tables = get_tables(txn)

    for table in tables:
        schema = get_schema(txn, table)
        if schema is None:
            continue

        foreign_keys = schema.get("foreign_keys", [])
        updated = False

        for fk in foreign_keys:
            # fk 예시: {"columns": [...], "ref_table": "student", "ref_columns": [...]}
            if fk.get("ref_table") == old_name:
                fk["ref_table"] = new_name
                updated = True

        if updated:
            # 변경된 schema 다시 저장
            create_schema(txn, schema)

##############################################################################################################
# 3. meta:table:<table_name>:counter
# 3.1. rename counter
def rename_counter(txn, old_name: str, new_name: str):
    """테이블의 counter 값을 새로운 이름의 키로 이동시킵니다."""
    old_key = f"meta:table:{old_name}:counter".encode()
    new_key = f"meta:table:{new_name}:counter".encode()
    
    val = txn.get(old_key)
    if val:
        txn.put(new_key, val)
        txn.delete(old_key)

def reset_counter(txn, table_name: str):
    """테이블의 ID 카운터를 초기화합니다."""
    counter_key = f"meta:table:{table_name}:counter".encode()
    # 카운터 키 자체를 삭제하면 add_row 로직에서 None으로 인식하여 0부터 다시 시작합니다.
    txn.delete(counter_key)

##############################################################################################################
# 4. data:<table_name>:<id>
# 4.1. add row
def add_row(txn, table_name: str, values: list):
    """
    테이블에 새로운 행을 추가합니다.
    counter를 조회하여 새로운 ID를 생성하고 데이터를 저장합니다.
    """
    # 1. 현재 counter 값을 가져오기 (없으면 0부터 시작)
    counter_key = f"meta:table:{table_name}:counter".encode()
    counter_bytes = txn.get(counter_key)
    
    if counter_bytes is None:
        current_id = 0
    else:
        current_id = int(counter_bytes.decode())
    
    # 2. 새로운 ID 생성 (1 증가)
    new_id = current_id + 1
    
    # 3. 데이터 저장 (Key: data:<table_name>:<id>)
    data_key = f"data:{table_name}:{new_id}".encode()
    # values는 truncate 처리가 완료된 리스트 형태라고 가정합니다.
    data_value = json.dumps(values).encode()
    
    txn.put(data_key, data_value)
    
    # 4. 업데이트된 counter 값을 다시 저장
    txn.put(counter_key, str(new_id).encode())

# 4.2. get rows
def get_rows(txn, table_name: str):
    """
    테이블의 모든 행을 가져오는 헬퍼 함수
    """
    prefix = f"data:{table_name}:".encode()
    cursor = txn.cursor()
    rows = []
    
    # KV 저장소의 특성을 이용해 prefix로 시작하는 모든 키-값 쌍을 순회
    for key, value in cursor:
        if key.startswith(prefix):
            rows.append(json.loads(value.decode()))
    
    return rows

# 4.3. rename data rows
def rename_data_rows(txn, old_name: str, new_name: str):
    """data:old_name:id 형식의 모든 데이터를 data:new_name:id로 이동시킵니다."""
    old_prefix = f"data:{old_name}:".encode()
    cursor = txn.cursor()
    
    # 커서를 사용해 기존 테이블의 모든 데이터를 순회
    for key, value in cursor:
        if key.startswith(old_prefix):
            # key 예시: b"data:account:1" -> "1" 추출
            row_id = key.decode().split(":")[-1]
            new_key = f"data:{new_name}:{row_id}".encode()
            
            # 새 이름으로 저장 후 기존 데이터 삭제
            txn.put(new_key, value)
            txn.delete(key)

# 4.4. Delete all rows
def delete_all_rows(txn, table_name: str):
    """해당 테이블의 모든 데이터 행을 삭제합니다."""
    prefix = f"data:{table_name}:".encode()
    cursor = txn.cursor()
    
    # 해당 프리픽스로 시작하는 모든 키를 순회하며 삭제
    for key, _ in cursor:
        if key.startswith(prefix):
            txn.delete(key)