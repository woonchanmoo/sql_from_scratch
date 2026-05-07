from config.messages.errors import *
from config.tools.basic_tools import *
import re


##################################################
# 🔹 COMMON UTIL
##################################################

# 두 컬럼 타입이 동일한지 비교 (int/date는 직접 비교, char는 base+length 모두 일치해야 함)
def is_same_type(t1, t2):
    if t1 == t2:
        return True

    if isinstance(t1, dict) and isinstance(t2, dict):
        return t1["base"] == t2["base"] and t1["length"] == t2["length"]

    return False


##################################################
# 🔹 CREATE TABLE VALIDATION
##################################################

# 1. TableExistenceError: Create table has failed: table with the same name already exists
def validate_table_not_exists(txn, table_name):
    tables = get_tables(txn)
    if table_name in tables:
        raise TableExistenceError()

# 2. DuplicateColumnDefError: Create table has failed: column definition is duplicated
def validate_columns(columns):
    seen = set()
    for colName in columns:
        if colName in seen:
            raise DuplicateColumnDefError()
        seen.add(colName)

# 3. CharLengthError: Char length should be over 0
def validate_char_length(schema):
    for _, info in schema["columns"].items():
        t = info["type"]

        if isinstance(t, dict) and t["base"] == "char":
            if t["length"] <= 0:
                raise CharLengthError()

# 4. DuplicatePrimaryKeyDefError: Create table has failed: primary key definition is duplicated
def validate_pk_duplicates(schema):
    pk_defs = schema["primary_keys"]

    # 1. PK 정의가 여러 번
    if len(pk_defs) > 1:
        raise DuplicatePrimaryKeyDefError()

    # 2. PK 내부 컬럼 중복
    if pk_defs:
        cols = pk_defs[0]
        if len(cols) != len(set(cols)):
            raise DuplicatePrimaryKeyDefError()

# 5. PrimaryKeyColumnDefError: Create table has failed:cannot define non-existing column '#colName' as primary key
def validate_primary_keys(schema):
    for col in schema["primary_keys"][0]:
        if col not in schema["columns"]:
            raise PrimaryKeyColumnDefError(col)

        # PK → NOT NULL 자동 설정
        schema["columns"][col]["not_null"] = True

# 6. ForeignKeyColumnDefError: Create table has failed: cannot define non-existing column '#colName' as foreign key
def validate_fk_column_existence(schema):
    for fk in schema["foreign_keys"]:
        for col in fk["columns"]:
            if col not in schema["columns"]:
                raise ForeignKeyColumnDefError(col)

# 7. FK column 중복
def validate_fk_duplicates(schema):
    for fk in schema["foreign_keys"]:
        cols = fk["columns"]
        if len(cols) != len(set(cols)):
            raise ForeignKeyColumnDefError(cols[0])

# 8. ReferenceExistenceError (column): Create table has failed: foreign key references non existing table or column
def validate_reference_columns(ref_schema, fk):
    for col in fk["ref_columns"]:
        if col not in ref_schema["column_names"]:
            raise ReferenceExistenceError()

# 9. ReferenceTypeError (개수 mismatch 포함): Create table has failed: foreign key references wrong type
def validate_fk_column_length(fk):
    if len(fk["columns"]) != len(fk["ref_columns"]):
        raise ReferenceTypeError()
    
# 10. ReferenceTypeError (type mismatch)
def validate_fk_type(schema, ref_schema, fk):
    for c, rc in zip(fk["columns"], fk["ref_columns"]):
        if rc not in ref_schema["columns"]:
            raise ReferenceExistenceError()

        t1 = schema["columns"][c]["type"]
        t2 = ref_schema["columns"][rc]["type"]

        if not is_same_type(t1, t2):
            raise ReferenceTypeError()

# 11. ReferenceNonPrimaryKeyError: Create table has failed: foreign key references non primary key column
def validate_fk_references_pk(ref_schema, fk):
    pk_defs = ref_schema["primary_keys"]

    if not pk_defs:
        raise ReferenceNonPrimaryKeyError()

    ref_pk = pk_defs[0]   # 🔥 핵심: list of columns

    ref_cols = fk["ref_columns"]

    if set(ref_cols) != set(ref_pk):
        raise ReferenceNonPrimaryKeyError()


##################################################
# 🔹 DROP TABLE VALIDATION
##################################################

# NoSuchTable: (#commandName) has failed: no such table
def validate_table_exists(txn, table_name, command_name):
    tables = get_tables(txn)
    if table_name not in tables:
        raise NoSuchTable(command_name)


# DropReferencedTableError: Drop table has failed: ‘#tableName’ is referenced by another table
def validate_drop_table(txn, table_name):
    # 다른 테이블의 FK가 해당 테이블을 참조하면 DROP 불가
    tables = get_tables(txn)

    for t in tables:
        schema = get_schema(txn, t)
        if not schema:
            continue

        for fk in schema.get("foreign_keys", []):
            if fk["ref_table"] == table_name:
                raise DropReferencedTableError(table_name)


##################################################
# 🔹 TRUNCATE TABLE VALIDATION
##################################################

# TruncateReferencedTableError: Truncate table has failed: ‘#tableName’ is referenced by another table
def validate_truncate_table(txn, table_name):
    # 다른 테이블의 FK가 해당 테이블을 참조하면 TRUNCATE 불가
    tables = get_tables(txn)

    for t in tables:
        schema = get_schema(txn, t)
        if not schema:
            continue

        for fk in schema.get("foreign_keys", []):
            if fk["ref_table"] == table_name:
                raise TruncateReferencedTableError(table_name)


##################################################
# 🔹 RENAME TABLE VALIDATION
##################################################

# RenameAlreadyExistError: Rename table has failed: there is already a table named ‘#newTableName’
def validate_rename_table(txn, new_table_name):
    # 변경할 이름이 이미 존재하면 RENAME 불가
    tables = get_tables(txn)
    if new_table_name in tables:
        raise RenameAlreadyExistError(new_table_name)

# 3-1


##################################################
# 🔹 SELECT VALIDATION
##################################################

# SelectTableExistenceError: Select has failed: '#tableName' does not exist
def validate_select_table(txn, table_name):
    tables = get_tables(txn)
    if table_name not in tables:
        raise SelectTableExistenceError(table_name)


# ORCHESTRATORS
##############################################################################################################################
##############################################################################################################################
##############################################################################################################################
##############################################################################################################################
##############################################################################################################################

##################################################
# Validate CREATE TABLE
##################################################
def validate_create(txn, schema):
    # CREATE TABLE 처리 시 필요한 모든 제약 조건을 순차적으로 확인

    # 1. 테이블 이름 중복 확인
    validate_table_not_exists(txn, schema["table_name"])
    
    # 2. 컬럼 중복 확인
    validate_columns(schema["column_names"])

    # 3.
    validate_char_length(schema)

    pks = schema["primary_keys"]
    fks = schema["foreign_keys"]

    if pks:
        # 4. 
        validate_pk_duplicates(schema)

        # 5. 
        validate_primary_keys(schema)

    if fks:
        # 6, 7.
        validate_fk_column_existence(schema)
        validate_fk_duplicates(schema)


        for fk in schema["foreign_keys"]:
            ref_schema = get_schema(txn, fk["ref_table"])
            if ref_schema is None:
                raise ReferenceExistenceError()

            # 8.
            # validate_reference_table(txn, fk)
            validate_reference_columns(ref_schema, fk)

            # 9, 10.
            validate_fk_column_length(fk)
            validate_fk_type(schema, ref_schema, fk)

            # 11.
            validate_fk_references_pk(ref_schema, fk)

##################################################
# Validate DROP
##################################################
def validate_drop(txn, table_name):
    validate_table_exists(txn, table_name, "Drop table")
    validate_drop_table(txn, table_name)

##################################################
# Validate EXPLAIN
##################################################
def validate_explain(txn, table_name):
    validate_table_exists(txn, table_name, "Explain")

def validate_describe(txn, table_name):
    validate_table_exists(txn, table_name, "Describe")

def validate_desc(txn, table_name):
    validate_table_exists(txn, table_name, "Desc")

##################################################
# Validate TRUNCATE
##################################################
def validate_truncate(txn, table_name):
    validate_table_exists(txn, table_name, "Truncate table")
    validate_truncate_table(txn, table_name)

##################################################
# Validate RENAME
##################################################
def validate_rename(txn, old_name, new_name):
    validate_table_exists(txn, old_name, "Rename table")
    validate_rename_table(txn, new_name)

##################################################
# Validate INSERT
##################################################
def validate_insert(txn, table_name, input_columns, values, all_column_names, processed_values, columns_info):
    validate_table_exists(txn, table_name, "Insert into")
    # Additional INSERT-specific validations are handled by helper functions.
    # Implement these helper functions later to support count, existence,
    # type mismatch, and non-null constraints.

    validate_insert_column_count(input_columns, values, all_column_names)

    validate_insert_column_existence(input_columns, all_column_names)

    validate_insert_type_mismatch(all_column_names, processed_values, columns_info)

    validate_insert_non_nullable(all_column_names, processed_values, columns_info)


def validate_insert_column_count(input_columns, values, all_column_names):
    """Validate the number of columns and values for INSERT."""
    if input_columns is None:
        # No explicit column list: values must match the table's full columns.
        if len(values) != len(all_column_names):
            raise InsertTypeMismatchError()
    else:
        # Explicit column list: each specified column must have a matching value.
        if len(input_columns) != len(values):
            raise InsertTypeMismatchError()


def validate_insert_column_existence(input_columns, all_column_names):
    """Validate that all explicitly listed columns exist."""
    if input_columns is not None:
        for col in input_columns:
            if col not in all_column_names:
                raise InsertColumnExistenceError(col)


def validate_insert_type_mismatch(all_column_names, processed_values, columns_info):
    """Validate that provided values match the target column types."""
    for i, col_name in enumerate(all_column_names):
        val = processed_values[i]
        col_type = columns_info[col_name]["type"]

        if val is not None:
            if col_type == "int":
                if not isinstance(val, int):
                    raise InsertTypeMismatchError()
            elif col_type == "date":
                if not isinstance(val, str) or not re.match(r'^\d{4}-\d{2}-\d{2}$', val):
                    raise InsertTypeMismatchError()
            elif isinstance(col_type, dict) and col_type.get("base") == "char":
                if not isinstance(val, str):
                    raise InsertTypeMismatchError()
            # Add more type checks as needed


def validate_insert_non_nullable(all_column_names, processed_values, columns_info):
    """Validate that non-nullable columns are not assigned NULL."""
    for i, col_name in enumerate(all_column_names):
        if columns_info[col_name]["not_null"] and processed_values[i] is None:
            raise InsertColumnNonNullableError(col_name)
        
##################################################
# Validate DELETE
##################################################
def validate_delete(txn, table_name, where_clause=None):
    """
    DELETE FROM table [WHERE clause] 검증
    
    검증 항목:
    1. 테이블 존재 확인 → NoSuchTable
    2. WHERE 절이 있다면:
       a. WHERE 절에서 참조하는 컬럼이 테이블에 존재하는지 확인
       b. WHERE 절의 기본 문법 검사 (추후 확장 가능)
    
    구조:
    - validate_table_exists(txn, table_name): 테이블 존재 확인
    - validate_where_columns(txn, table_name, where_clause): WHERE절 컬럼 검증
    """
    
    # 1. 테이블 존재 확인
    validate_table_exists(txn, table_name, "Delete")
    
    # 2. WHERE 절 검증 (있으면)
    if where_clause is not None:
        validate_where_columns(txn, table_name, where_clause)


def validate_where_columns(txn, table_name, where_clause):
    """
    WHERE clause에서 참조하는 컬럼들이 테이블에 존재하는지 확인합니다.

    Raises:
        TableNotSpecified : col_ref.table이 FROM절 테이블과 다를 때
        ColumnNotExist    : 컬럼이 스키마에 없을 때
        IncomparableError : 타입/연산자가 호환되지 않을 때
    """
    schema = get_schema(txn, table_name)
    _validate_where_node(where_clause, table_name, schema)


def _validate_where_node(node, table_name, schema):
    """WHERE AST 노드를 재귀적으로 순회하며 검증한다."""
    t = node["type"]
    if t in ("and", "or"):
        for operand in node["operands"]:
            _validate_where_node(operand, table_name, schema)
    elif t == "comparison":
        # 컬럼 참조 검증
        if node["left"]["type"] == "column":
            _validate_column_ref(node["left"], table_name, schema)
        if node["right"]["type"] == "column":
            _validate_column_ref(node["right"], table_name, schema)
        # 타입/연산자 호환성 검증
        _validate_comparison_types(node, schema)
    elif t == "null_predicate":
        _validate_column_ref(node["column"], table_name, schema)


def _validate_column_ref(col_ref, table_name, schema):
    """컬럼 참조의 테이블 prefix 및 컬럼 존재 여부를 검증한다."""
    # table prefix가 명시되었고 현재 테이블과 다르면 TableNotSpecified
    if col_ref["table"] is not None and col_ref["table"] != table_name:
        raise TableNotSpecified("where")
    # 컬럼이 스키마에 없으면 ColumnNotExist
    if col_ref["column"] not in schema["columns"]:
        raise ColumnNotExist("where")


def _validate_comparison_types(comparison, schema):
    """
    comparison 노드의 양쪽 operand 타입과 연산자의 호환성을 검증한다.

    타입 규칙 (Table 1):
        char        : = != 만 허용
        int / date  : 모든 비교 연산자 허용
        str_literal : char·date 컬럼 모두와 호환 (Python str 리터럴)
        null 리터럴 : comparison에서는 항상 IncomparableError
    """
    left, right, op = comparison["left"], comparison["right"], comparison["operator"]

    l_type = _get_operand_schema_type(left, schema)   # "int"|"date"|"char"|"str_literal"|None
    r_type = _get_operand_schema_type(right, schema)

    # null 리터럴이 comparison에 있으면 IS NULL 을 써야 하므로 IncomparableError
    if l_type is None or r_type is None:
        raise IncomparableError()

    # 유효 타입 결정
    if l_type == "str_literal" and r_type == "str_literal":
        effective = "char"                  # 두 문자열 리터럴 → char 문맥
    elif l_type == "str_literal":
        if r_type == "int":                 # 문자열 리터럴 vs int 컬럼 → 불가
            raise IncomparableError()
        effective = r_type                  # char 또는 date 컬럼 타입 따라감
    elif r_type == "str_literal":
        if l_type == "int":
            raise IncomparableError()
        effective = l_type
    else:
        if l_type != r_type:               # int vs date 등 서로 다른 컬럼 타입
            raise IncomparableError()
        effective = l_type

    # char 타입에서 대소 비교 연산자 사용 금지
    if effective == "char" and op not in ("=", "!="):
        raise IncomparableError()


def _get_operand_schema_type(operand, schema):
    """operand의 정적 타입을 반환한다. ("int"|"date"|"char"|"str_literal"|None)"""
    if operand["type"] == "column":
        col_type = schema["columns"][operand["column"]]["type"]
        if isinstance(col_type, dict):      # {"base": "char", "length": N}
            return "char"
        return col_type                     # "int" 또는 "date"
    elif operand["type"] == "value":
        val = operand["value"]
        if isinstance(val, int):
            return "int"
        elif isinstance(val, str):
            return "str_literal"            # char·date 모두와 호환
        return None                         # null



##################################################
# Validate SELECT
##################################################
def validate_select(txn, table_name):
    validate_select_table(txn, table_name)


def validate_select_query(txn, select_schema, schemas):
    """
    SELECT 쿼리의 전체 절을 순서대로 검증한다.

    검증 순서:
    1. FROM 테이블 존재 확인   → SelectTableExistenceError(table_name)
    2. JOIN ON 컬럼 검증       → ColumnNotExist("join"), IncomparableError
    3. WHERE 절 검증           → TableNotSpecified("where"), ColumnNotExist("where"),
                                  AmbiguousReference("where"), IncomparableError
    4. GROUP BY 절 검증        → TableNotSpecified("group by"), ColumnNotExist("group by"),
                                  AmbiguousReference("group by")
    5. ORDER BY 절 검증        → ColumnNotExist("order by"), AmbiguousReference("order by")
    6. LIMIT / OFFSET 값 검증  → InvalidLimitOffsetError
    7. SELECT 컬럼 목록 검증   → SelectColumnResolveError(col_name),
                                  SelectColumnNotGrouped(col_name)

    Args:
        txn          : LMDB transaction
        select_schema: SELECT AST dict (sql_transformer 출력)
        schemas      : {table_name: schema_dict}  (_load_table_schemas 결과)
    """
    from_list    = select_schema["from_list"]
    join_list    = select_schema.get("join_list", [])
    where_clause = select_schema.get("where_clause")
    group_by     = select_schema.get("group_by")
    order_by     = select_schema.get("order_by")
    select_list  = select_schema["select_list"]
    limit        = select_schema.get("limit")
    offset       = select_schema.get("offset")

    # FROM + JOIN 테이블 전체 목록 (WHERE/GROUP BY/ORDER BY/SELECT 검증에 사용)
    all_tables = list(from_list)
    for j in join_list:
        if j["table"] not in all_tables:
            all_tables.append(j["table"])

    # 1. FROM 테이블 존재 확인
    _validate_select_table_existence(txn, from_list)

    # 2. JOIN ON 컬럼 검증
    if join_list:
        _validate_join_columns(join_list, schemas, from_list)

    # 3. WHERE 절 검증
    if where_clause:
        _validate_where_select(where_clause, schemas, all_tables)

    # 4. GROUP BY 절 검증
    if group_by:
        _validate_group_by_column(group_by, schemas, all_tables)

    # 5. ORDER BY 절 검증
    if order_by:
        _validate_order_by_column(order_by, schemas, all_tables, select_list)

    # 6. LIMIT / OFFSET 값 검증
    _validate_limit_offset_values(limit, offset)

    # 7. SELECT 컬럼 목록 검증
    _validate_select_columns(select_list, group_by, schemas, all_tables)


# ──────────────────────────────────────────────
# SELECT 검증 내부 헬퍼
# ──────────────────────────────────────────────

def _validate_select_table_existence(txn, from_list):
    """FROM 테이블 존재 확인 (_load_table_schemas 이전에 실행)."""
    tables = get_tables(txn)
    for name in from_list:
        if name not in tables:
            raise SelectTableExistenceError(name)


def _resolve_column_in_schemas(col_ref, schemas, from_list, clause_name):
    """col_ref를 (table_name, col_name) 으로 해석한다.
    table prefix가 있으면 해당 테이블만 확인, 없으면 from_list 전체 탐색.
    """
    table_ref = col_ref.get("table")
    col_name  = col_ref["column"]

    if table_ref is not None:
        # 명시된 테이블이 FROM/JOIN에 없으면 TableNotSpecified
        if table_ref not in from_list:
            raise TableNotSpecified(clause_name)
        # 해당 테이블에 컬럼이 없으면 ColumnNotExist
        if col_name not in schemas.get(table_ref, {}).get("columns", {}):
            raise ColumnNotExist(clause_name)
        return (table_ref, col_name)

    # table prefix 없음 → 모든 from_list 테이블에서 탐색
    matching = [t for t in from_list if col_name in schemas.get(t, {}).get("columns", {})]
    if len(matching) == 0:
        raise ColumnNotExist(clause_name)
    if len(matching) > 1:
        raise AmbiguousReference(clause_name)
    return (matching[0], col_name)


def _get_operand_type_multi(operand, schemas, from_list):
    """operand의 정적 타입을 반환한다 ("int"|"date"|"char"|"str_literal"|None).
    리터럴 값은 Python 타입으로 판별하고, 컬럼 참조는 스키마에서 조회한다.
    """
    if operand["type"] == "value":
        val = operand["value"]
        if isinstance(val, int):   return "int"
        if isinstance(val, str):   return "str_literal"
        return None                # null literal
    # column 참조: 스키마에서 타입 조회
    try:
        table_name, col_name = _resolve_column_in_schemas(operand, schemas, from_list, "where")
        col_type = schemas[table_name]["columns"][col_name]["type"]
        return "char" if isinstance(col_type, dict) else col_type
    except Exception:
        return None


def _validate_comparison_types_multi(comparison, schemas, from_list):
    """multi-table 컨텍스트에서 comparison 타입·연산자 호환성을 검증한다.
    char 타입에는 대소 비교 불가, null 리터럴이 comparison에 오면 IncomparableError.
    """
    left, right, op = comparison["left"], comparison["right"], comparison["operator"]
    l_type = _get_operand_type_multi(left,  schemas, from_list)
    r_type = _get_operand_type_multi(right, schemas, from_list)

    # null 리터럴이 포함된 comparison은 항상 IncomparableError
    if l_type is None or r_type is None:
        raise IncomparableError()

    # str_literal은 char/date 컬럼 모두와 호환; int와는 불가
    if l_type == "str_literal" and r_type == "str_literal":
        effective = "char"
    elif l_type == "str_literal":
        if r_type == "int": raise IncomparableError()
        effective = r_type
    elif r_type == "str_literal":
        if l_type == "int": raise IncomparableError()
        effective = l_type
    else:
        if l_type != r_type: raise IncomparableError()
        effective = l_type

    # char 타입에서 대소 비교 연산자 사용 금지
    if effective == "char" and op not in ("=", "!="):
        raise IncomparableError()


def _validate_where_node_select(node, schemas, from_list):
    """WHERE AST 노드를 재귀 검증한다 (다중 테이블).
    and/or는 재귀, comparison은 컬럼 존재·타입 검증, null_predicate는 컬럼 존재 검증.
    """
    t = node["type"]
    if t in ("and", "or"):
        for op in node["operands"]:
            _validate_where_node_select(op, schemas, from_list)
    elif t == "comparison":
        if node["left"]["type"]  == "column":
            _resolve_column_in_schemas(node["left"],  schemas, from_list, "where")
        if node["right"]["type"] == "column":
            _resolve_column_in_schemas(node["right"], schemas, from_list, "where")
        _validate_comparison_types_multi(node, schemas, from_list)
    elif t == "null_predicate":
        _resolve_column_in_schemas(node["column"], schemas, from_list, "where")


def _validate_where_select(where_clause, schemas, from_list):
    """SELECT WHERE 절 검증 (다중 테이블) 진입점."""
    _validate_where_node_select(where_clause, schemas, from_list)


def _validate_join_columns(join_list, schemas, from_list):
    """JOIN ON 컬럼 존재 및 타입 호환성 검증.
    누적 JOIN을 지원하기 위해 JOIN 테이블을 available에 순서대로 추가한다.
    """
    available = list(from_list)
    for jc in join_list:
        join_table = jc["table"]
        # JOIN 테이블도 available에 추가 (누적 JOIN 지원)
        if join_table not in available:
            available.append(join_table)

        try:
            lt, lc = _resolve_column_in_schemas(jc["left"],  schemas, available, "join")
            rt, rc = _resolve_column_in_schemas(jc["right"], schemas, available, "join")
        except ColumnNotExist:
            raise ColumnNotExist("join")

        # ON 조건의 두 컬럼은 동일한 타입이어야 함
        l_type = schemas[lt]["columns"][lc]["type"]
        r_type = schemas[rt]["columns"][rc]["type"]
        if not is_same_type(l_type, r_type):
            raise IncomparableError()


def _validate_group_by_column(group_by, schemas, from_list):
    """GROUP BY 컬럼 존재 및 모호성 검증."""
    col_ref = {"table": group_by.get("table"), "column": group_by["column"]}
    _resolve_column_in_schemas(col_ref, schemas, from_list, "group by")


def _validate_order_by_column(order_by, schemas, from_list, select_list):
    """ORDER BY 컬럼 검증.
    SELECT 목록의 alias/컬럼명을 우선 탐색하고, 없으면 실제 테이블 컬럼에서 탐색한다.
    """
    col_name = order_by["column"]

    # SELECT 목록의 alias / 컬럼명에서 먼저 찾기
    select_names = set()
    for item in select_list:
        if item.get("alias"):
            select_names.add(item["alias"])
        if item.get("column"):
            select_names.add(item["column"])
    if col_name in select_names:
        return

    # alias에 없으면 테이블 스키마에서 탐색 (존재·모호성 검증 포함)
    col_ref = {"table": order_by.get("table"), "column": col_name}
    _resolve_column_in_schemas(col_ref, schemas, from_list, "order by")


def _validate_limit_offset_values(limit, offset):
    """LIMIT / OFFSET 값이 음수이면 InvalidLimitOffsetError를 발생시킨다."""
    if limit  is not None and limit  < 0: raise InvalidLimitOffsetError()
    if offset is not None and offset < 0: raise InvalidLimitOffsetError()


def _validate_select_columns(select_list, group_by, schemas, from_list):
    """SELECT 컬럼 존재·모호성·GROUP BY 제약 검증."""
    gb_col   = group_by["column"]           if group_by else None
    gb_table = group_by.get("table")        if group_by else None

    for item in select_list:
        if item["type"] == "star":
            if group_by:
                # star는 모든 컬럼을 포함하므로 GROUP BY 컬럼 외 비집계 컬럼이 반드시 존재함
                for tbl in from_list:
                    for col in schemas.get(tbl, {}).get("column_names", []):
                        is_gb = (col == gb_col and (gb_table is None or tbl == gb_table))
                        if not is_gb:
                            raise SelectColumnNotGrouped(col)
            continue

        col_name  = item.get("column")
        table_ref = item.get("table")

        if item["type"] == "column":
            try:
                resolved_t, resolved_c = _resolve_column_in_schemas(
                    {"table": table_ref, "column": col_name}, schemas, from_list, "select"
                )
            except (ColumnNotExist, AmbiguousReference, TableNotSpecified):
                raise SelectColumnResolveError(col_name)

            # GROUP BY 제약: 비집계 컬럼은 반드시 GROUP BY 컬럼이어야 함
            if group_by:
                gb_matches_col = (resolved_c == gb_col)
                gb_matches_tbl = (gb_table is None or resolved_t == gb_table)
                if not (gb_matches_col and gb_matches_tbl):
                    raise SelectColumnNotGrouped(col_name)

        elif item["type"] == "aggregate":
            # 집계 컬럼 존재 확인
            try:
                _resolve_column_in_schemas(
                    {"table": table_ref, "column": col_name}, schemas, from_list, "select"
                )
            except (ColumnNotExist, AmbiguousReference, TableNotSpecified):
                raise SelectColumnResolveError(col_name)