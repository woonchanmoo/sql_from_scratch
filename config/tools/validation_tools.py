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
# Validate SELECT
##################################################
def validate_select(txn, table_name):
    validate_select_table(txn, table_name)

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