class SyntaxError(Exception): pass

class DuplicateColumnDefError(Exception): pass

class DuplicatePrimaryKeyDefError(Exception): pass

class ReferenceTypeError(Exception): pass

class ReferenceNonPrimaryKeyError(Exception): pass

class ReferenceExistenceError(Exception): pass

class PrimaryKeyColumnDefError(Exception):
    def __init__(self, colName):
        self.colName = colName

class ForeignKeyColumnDefError(Exception):
    def __init__(self, colName):
        self.colName = colName

class TableExistenceError(Exception): pass

class CharLengthError(Exception): pass

class NoSuchTable(Exception):
    def __init__(self, commandName):
        self.commandName = commandName

class DropReferencedTableError(Exception):
    def __init__(self, tableName):
        self.tableName = tableName

class SelectTableExistenceError(Exception):
    def __init__(self, tableName):
        self.tableName = tableName

class RenameAlreadyExistError(Exception):
    def __init__(self, newTableName):
        self.newTableName = newTableName
        
class TruncateReferencedTableError(Exception):
    def __init__(self, tableName):
        self.tableName = tableName

