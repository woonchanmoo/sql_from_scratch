# message.py
from config.messages.errors import *

# Result 객체는 쿼리 실행 성공 시 반환할 결과 타입과 데이터를 담는다.
class Result:
    def __init__(self, type, data=None):
        self.type = type
        self.data = data

class ExecutionResult:
    def __init__(self, result=None, error=None):
        self.result = result
        self.error = error

    @property
    def is_success(self):
        # error가 없으면 성공으로 간주
        return self.error is None

# ------------------------
# 성공 메시지
# ------------------------

def format_success(res: Result):
    """성공 결과 객체를 사용자 친화적인 메시지 문자열로 변환."""
    if res.type == "CreateTableSuccess":
        return f"'{res.data}' table is created"

    elif res.type == "DropSuccess":
        return f"'{res.data}' table is dropped"

    elif res.type == "InsertResult":
        return "1 row inserted"

    elif res.type == "RenameSuccess":
        return f"'{res.data}' is renamed"

    elif res.type == "TruncateSuccess":
        return f"'{res.data}' is truncated"
    
    elif res.type == "ExplainSuccess":
        return ""
    
    elif res.type == "ShowTablesSuccess":
        return ""
    
    elif res.type == "InsertResult":
        return "The row is inserted"
    
    elif res.type == "SelectSuccess":
        return ""

    # 1-3
    elif res.type == "DeleteResult":
        return f"'{res.data}' row(s) deleted"

    elif res.type == "DeleteReferentialIntegrityPassed":
        return f"'{res.data}' row(s) are not deleted due to referential integrity"

    else:
        return "Unknown success"


# ------------------------
# 에러 메시지
# ------------------------

def format_error(e: Exception):
    """예외 객체를 사용자에게 보여줄 에러 메시지로 변환."""
    if isinstance(e, SyntaxError):
        return "Syntax error"

    elif isinstance(e, DuplicateColumnDefError):
        return "Create table has failed: column definition is duplicated"

    elif isinstance(e, DuplicatePrimaryKeyDefError):
        return "Create table has failed: primary key definition is duplicated"

    elif isinstance(e, ReferenceTypeError):
        return "Create table has failed: foreign key references wrong type"

    elif isinstance(e, ReferenceNonPrimaryKeyError):
        return "Create table has failed: foreign key references non primary key column"

    elif isinstance(e, ReferenceExistenceError):
        return "Create table has failed: foreign key references non existing table or column"

    elif isinstance(e, PrimaryKeyColumnDefError):
        return f"Create table has failed: cannot define non-existing column '{e.colName}' as primary key"

    elif isinstance(e, ForeignKeyColumnDefError):
        return f"Create table has failed: cannot define non-existing column '{e.colName}' as foreign key"

    elif isinstance(e, TableExistenceError):
        return "Create table has failed: table with the same name already exists"

    elif isinstance(e, CharLengthError):
        return "Char length should be over 0"

    elif isinstance(e, NoSuchTable):
        return f"{e.commandName} has failed: no such table"

    elif isinstance(e, DropReferencedTableError):
        return f"Drop table has failed: '{e.tableName}' is referenced by another table"

    elif isinstance(e, SelectTableExistenceError):
        return f"Select has failed: '{e.tableName}' does not exist"

    elif isinstance(e, RenameAlreadyExistError):
        return f"Rename table has failed: there is already a table named '{e.newTableName}'"

    elif isinstance(e, TruncateReferencedTableError):
        return f"Truncate table has failed: '{e.tableName}' is referenced by another table"

    # 1-3

    elif isinstance(e, InsertTypeMismatchError):
        return "Insert has failed: types are not matched"

    elif isinstance(e, InsertColumnExistenceError):
        return f"Insert has failed: '{e.colName}' does not exist"

    elif isinstance(e, InsertColumnNonNullableError):
        return f"Insert has failed: '{e.colName}' is not nullable"

    elif isinstance(e, SelectColumnResolveError):
        return f"Select has failed: fail to resolve '{e.colName}'"

    elif isinstance(e, SelectColumnNotGrouped):
        return f"Select has failed: column '{e.colName}' must either be included in the GROUP BY clause or be used in an aggregate function"

    elif isinstance(e, TableNotSpecified):
        return f"{e.clauseName} clause trying to reference tables which are not specified"

    elif isinstance(e, ColumnNotExist):
        return f"{e.clauseName} clause trying to reference non existing column"

    elif isinstance(e, AmbiguousReference):
        return f"{e.clauseName} clause contains ambiguous column reference"

    elif isinstance(e, IncomparableError):
        return "Trying to compare incomparable columns or values"

    elif isinstance(e, InvalidLimitOffsetError):
        return "Select has failed: LIMIT/OFFSET clause should be a non-negative integer"

    else:
        return f"unknown error in validation: {e}"