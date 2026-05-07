# errors.py
# 사용자 정의 예외 타입을 선언하여 validation 및 쿼리 실행 실패를 구분합니다.

# SQL 문법 오류
class SyntaxError(Exception): pass

# CREATE TABLE: 컬럼 이름 중복 정의
class DuplicateColumnDefError(Exception): pass

# CREATE TABLE: PK 중복 정의
class DuplicatePrimaryKeyDefError(Exception): pass

# CREATE TABLE: FK 참조 컬럼 타입 불일치
class ReferenceTypeError(Exception): pass

# CREATE TABLE: FK가 PK가 아닌 컬럼을 참조
class ReferenceNonPrimaryKeyError(Exception): pass

# CREATE TABLE: FK 참조 테이블 또는 컬럼이 존재하지 않음
class ReferenceExistenceError(Exception): pass

# CREATE TABLE: 존재하지 않는 컬럼을 PK로 정의
class PrimaryKeyColumnDefError(Exception):
    def __init__(self, colName):
        self.colName = colName

# CREATE TABLE: 존재하지 않는 컬럼을 FK로 정의
class ForeignKeyColumnDefError(Exception):
    def __init__(self, colName):
        self.colName = colName

# CREATE TABLE: 동일한 이름의 테이블이 이미 존재
class TableExistenceError(Exception): pass

# CREATE TABLE: char 타입 길이가 0 이하
class CharLengthError(Exception): pass

# 존재하지 않는 테이블 참조 (Drop/Delete 등)
class NoSuchTable(Exception):
    def __init__(self, commandName):
        self.commandName = commandName

# DROP TABLE: FK로 참조 중인 테이블 삭제 시도
class DropReferencedTableError(Exception):
    def __init__(self, tableName):
        self.tableName = tableName

# SELECT: FROM 절에서 존재하지 않는 테이블 참조
class SelectTableExistenceError(Exception):
    def __init__(self, tableName):
        self.tableName = tableName

# RENAME TABLE: 변경할 이름이 이미 존재
class RenameAlreadyExistError(Exception):
    def __init__(self, newTableName):
        self.newTableName = newTableName

# TRUNCATE TABLE: FK로 참조 중인 테이블 TRUNCATE 시도
class TruncateReferencedTableError(Exception):
    def __init__(self, tableName):
        self.tableName = tableName

# DB 1-3

# INSERT: 값의 타입 또는 개수 불일치
class InsertTypeMismatchError(Exception): pass

# INSERT: 존재하지 않는 컬럼 지정
class InsertColumnExistenceError(Exception):
    def __init__(self, colName):
        self.colName = colName

# INSERT: NOT NULL 컬럼에 NULL 삽입 시도
class InsertColumnNonNullableError(Exception):
    def __init__(self, colName):
        self.colName = colName

# SELECT: 컬럼 참조 해석 실패 (존재하지 않거나 모호한 컬럼)
class SelectColumnResolveError(Exception):
    def __init__(self, colName):
        self.colName = colName

# SELECT: GROUP BY에 포함되지 않은 비집계 컬럼 선택
class SelectColumnNotGrouped(Exception):
    def __init__(self, colName):
        self.colName = colName

# WHERE/JOIN: 지정되지 않은 테이블을 참조
class TableNotSpecified(Exception):
    def __init__(self, clauseName):
        self.clauseName = clauseName

# WHERE/JOIN: 존재하지 않는 컬럼 참조
class ColumnNotExist(Exception):
    def __init__(self, clauseName):
        self.clauseName = clauseName

# WHERE/JOIN: 어느 테이블인지 불명확한 컬럼 참조
class AmbiguousReference(Exception):
    def __init__(self, clauseName):
        self.clauseName = clauseName

# DELETE: FK 무결성 위반으로 일부 행 삭제 불가
class DeleteReferentialIntegrityPassed(Exception):
    def __init__(self, count):
        self.count = count

# 비교 불가능한 타입 또는 연산자 조합 사용
class IncomparableError(Exception): pass

# LIMIT/OFFSET: 음수 값 사용
class InvalidLimitOffsetError(Exception): pass