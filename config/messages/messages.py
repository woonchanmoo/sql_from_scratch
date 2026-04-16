# message.py
from config.messages.errors import *

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
        return self.error is None

# ------------------------
# 성공 메시지
# ------------------------

def format_success(res: Result):
    if res.type == "CreateTableSuccess":
        return f"'{res.data}' table is created"

    elif res.type == "DropSuccess":
        return f"'{res.data}' table is dropped"

    elif res.type == "InsertResult":
        return "The row is inserted"

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

    else:
        return "Unknown success"


# ------------------------
# 에러 메시지
# ------------------------

def format_error(e: Exception):
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

    else:
        return f"unknown error in validation: {e}"