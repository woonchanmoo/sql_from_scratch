from config.messages.errors import *
from config.tools.basic_tools import *
import re


##################################################
# 🔹 COMMON UTIL
##################################################

# 데이터 타입 비교를 지원하는 헬퍼 (int, date, char(n) 비교)
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
    for col, info in schema["columns"].items():
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


# DropReferencedTableError: Drop table has failed: '#tableName' is referenced by another table
def validate_drop_table(txn, table_name):
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