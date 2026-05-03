# SQL Database Engine - 프로젝트 아키텍처

## 📋 개요

이 프로젝트는 Python에서 SQL을 파싱하고 실행하는 데이터베이스 엔진입니다. Lark 파서를 사용하여 SQL 쿼리를 추상 문법 트리(AST)로 변환하고, 이를 Python 객체로 변환한 후 LMDB (Lightweight Embedded Database)에 저장합니다.

### 데이터 흐름

```
SQL 입력 (run.py)
    ↓
Lark Parser (grammar.lark)
    ↓
MyTransformer (config/transformer/sql_transformer.py) - AST → Python 객체 변환
    ↓
QueryExecutor (config/tools/executor.py) - 쿼리 타입 분기
    ↓
Query Handlers (config/tools/query_tools.py) - 실제 DB 실행
    ↓
Validation & Error Messages (config/tools/validation_tools.py, config/messages)
    ↓
LMDB 데이터베이스 (DB/myDB.mdb)
```

---

## 📁 파일 구조 및 역할

### 1. **run.py** - 메인 실행 엔트리 포인트

**역할**: SQL 입력을 받아 파싱 및 실행 주기를 관리합니다.

**주요 기능**:
- Lark 파서 초기화 (grammar.lark 로드)
- LMDB 환경 설정
- 사용자 입력 반복 처리 (multiline 지원)
- 쿼리 시퀀스 분리 및 실행
- 예외 처리 및 에러 메시지 출력

**예시**:
```python
# SQL 쿼리 입력
DB_2019-14473> CREATE TABLE student (
> student_id int NOT NULL,
> student_name char(15),
> PRIMARY KEY (student_id));

# 또는
DB_2019-14473> INSERT INTO student VALUES (1, 'Alice');

# 종료
DB_2019-14473> exit;
```

---

### 2. **grammar.lark** - SQL 문법 정의

**역할**: SQL 쿼리 문법을 정의합니다.

**지원하는 명령어**:
- `CREATE TABLE`
- `DROP TABLE`
- `EXPLAIN / DESCRIBE / DESC`
- `SHOW TABLES`
- `INSERT INTO ... VALUES`
- `SELECT ... FROM ... WHERE`
- `RENAME TABLE`
- `TRUNCATE TABLE`
- `UPDATE ... SET`
- `DELETE FROM`

**예시 (일부)**:
```
TYPE_INT : "int"i
TYPE_CHAR : "char"i
TYPE_DATE : "date"i

CREATE : "create"i
TABLE : "table"i
NOT : "not"i
NULL : "null"i
PRIMARY : "primary"i
FOREIGN : "foreign"i
```

---

### 3. **config/transformer/sql_transformer.py** - 파싱 결과 변환 (AST → Python 객체)

**역할**: Lark에서 파싱된 추상 문법 트리(Tree)를 Python 객체로 변환합니다.

**주요 메서드들**:

| 메서드 | 입력 | 출력 | 설명 |
|--------|------|------|------|
| `table_name()` | Token | str (lowercase) | 테이블 이름을 소문자로 정규화 |
| `column_name()` | Token | str (lowercase) | 컬럼 이름을 소문자로 정규화 |
| `data_type()` | Token | str or dict | `int`, `date` 또는 `{"base": "char", "length": N}` |
| `column_definition()` | items | dict | `{"type": "column", "name": ..., "data_type": ..., "not_null": ...}` |
| `primary_key_constraint()` | items | dict | `{"type": "pk", "columns": [...]}` |
| `referential_constraint()` | items | dict | `{"type": "fk", "columns": [...], "ref_table": ..., "ref_columns": [...]}` |
| `value_type()` | Token | int/str/None | INSERT VALUES 내 값 파싱 |
| `select_list()` | items | list | SELECT 절 컬럼 리스트 또는 `["*"]` |

**예시 - CREATE TABLE 파싱**:
```python
# SQL: CREATE TABLE student (student_id int NOT NULL, student_name char(15), PRIMARY KEY (student_id));

# MyTransformer 결과:
{
    "type": "create_table",
    "schema": {
        "table_name": "student",
        "columns": {
            "student_id": {
                "type": "int",
                "not_null": True
            },
            "student_name": {
                "type": {"base": "char", "length": 15},
                "not_null": False
            }
        },
        "column_names": ["student_id", "student_name"],
        "primary_keys": [["student_id"]],  # ← 중요: [[]] 형태 (리스트 안의 리스트)
        "foreign_keys": []
    }
}
```

**예시 - INSERT 파싱**:
```python
# SQL: INSERT INTO student (student_id, student_name) VALUES (1, 'Alice');

# MyTransformer 결과:
{
    "type": "insert",
    "insert_schema": {
        "table_name": "student",
        "column_names": ["student_id", "student_name"]
        "values": [1, "Alice"]
    }
}
```

---

### 4. **config/tools/executor.py** - 쿼리 실행 조정자

**역할**: 파싱된 쿼리를 적절한 핸들러로 라우팅합니다.

**주요 클래스**: `QueryExecutor`

**메서드**: `execute(query)`

**쿼리 타입별 처리**:
```python
qtype = query["type"]

if qtype == "create_table":
    return create_table(txn, query["schema"])

elif qtype == "insert":
    return insert_into_table(txn, query["insert_schema"])

elif qtype == "select":
    return select_table(txn, query["select_schema"])

elif qtype == "drop_table":
    return drop_table(txn, query["table_name"])

# ... 기타 타입들
```

**반환 값**: `ExecutionResult` 객체
- 성공 시: `ExecutionResult(result=Result(...), error=None)`
- 실패 시: `ExecutionResult(result=None, error=Exception(...))`

---

### 5. **config/tools/validation_tools.py** - 검증 및 에러 생성

**역할**: CREATE TABLE 스키마 검증 및 에러 메시지 생성합니다.

**주요 검증 함수들**:

| 함수 | 목적 | 발생 에러 |
|------|------|---------|
| `validate_table_not_exists()` | 테이블 중복 생성 방지 | `TableExistenceError` |
| `validate_columns()` | 컬럼 이름 중복 확인 | `DuplicateColumnDefError` |
| `validate_char_length()` | CHAR 타입 길이 >= 1 확인 | `CharLengthError` |
| `validate_pk_duplicates()` | PK 정의 중복 확인 | `DuplicatePrimaryKeyDefError` |
| `validate_primary_keys()` | PK 컬럼 존재 여부 확인 | `PrimaryKeyColumnDefError` |
| `validate_fk_column_existence()` | FK 컬럼 존재 여부 확인 | `ForeignKeyColumnDefError` |
| `validate_fk_duplicates()` | FK 컬럼 중복 확인 | `ForeignKeyColumnDefError` |
| `validate_reference_columns()` | FK 참조 컬럼 존재 확인 | `ReferenceExistenceError` |
| `validate_fk_column_length()` | FK-RefColumn 개수 일치 확인 | `ReferenceTypeError` |
| `validate_fk_type()` | FK-RefColumn 타입 일치 확인 | `ReferenceTypeError` |

**예시**:
```python
# CREATE TABLE student (id int, name char(-5));  
# → validate_char_length() 호출
# → CharLengthError 발생
# → "Char length should be over 0" 메시지 출력

# CREATE TABLE student (id int PRIMARY KEY PRIMARY KEY (id));
# → validate_pk_duplicates() 호출
# → DuplicatePrimaryKeyDefError 발생
# → "Create table has failed: primary key definition is duplicated" 메시지 출력

# CREATE TABLE student (id int, PRIMARY KEY (id, id));
# → validate_pk_duplicates() 호출 (내부 중복 확인)
# → DuplicatePrimaryKeyDefError 발생
```

---

### 6. **config/tools/basic_tools.py** - 메타데이터 관리 및 데이터 접근

**역할**: LMDB에서 테이블 정보, 스키마, 레코드를 가져오고 관리합니다.

#### 📌 LMDB 키 구조

| 키 패턴 | 값 | 설명 |
|---------|-----|------|
| `meta:tables` | JSON 문자열 (테이블 이름 정렬 리스트) | 모든 테이블 목록 |
| `meta:table:{table_name}:schema` | JSON 문자열 (스키마 객체) | 테이블 스키마 정의 |
| `meta:table:{table_name}:counter` | 정수 (바이트) | 다음 행 ID 값 |
| `data:{table_name}:{id}` | JSON 문자열 (레코드 객체) | 실제 데이터 행 |

#### 주요 함수들

**테이블 목록 관리**:
```python
def get_tables(txn) → set:
    """LMDB에서 모든 테이블 이름을 집합으로 반환"""
    # 키: b"meta:tables"
    # 예: {"student", "instructor", "course"}
    
def add_tables(txn, table_name: str):
    """새 테이블을 meta:tables에 추가"""
    # 기존 테이블 집합에 table_name 추가 후 정렬하여 저장
    
def remove_table(txn, table_name: str):
    """테이블을 meta:tables에서 제거"""
```

**스키마 관리**:
```python
def get_schema(txn, table_name: str) → dict:
    """테이블의 스키마를 반환"""
    # 키: b"meta:table:{table_name}:schema"
    # 예: {
    #   "table_name": "student",
    #   "columns": {...},
    #   "primary_keys": [[...]],
    #   "foreign_keys": [...]
    # }
    
def create_schema(txn, schema: dict):
    """스키마를 LMDB에 저장"""
    
def delete_schema(txn, table_name: str):
    """스키마 메타데이터 삭제"""
```

**데이터 행 관리**:
```python
def add_row(txn, table_name: str, values: list) → int:
    """새 행을 추가하고 행 ID 반환"""
    # 키: b"data:{table_name}:{id}"
    # 예: {
    #   "student_id": 1,
    #   "student_name": "Alice",
    #   "birth_date": "2000-01-15"
    # }
    
def get_row(txn, table_name: str, row_id: int) → dict:
    """특정 행을 반환"""
    
def delete_row(txn, table_name: str, row_id: int):
    """특정 행 삭제"""
    
def update_row(txn, table_name: str, row_id: int, values: dict):
    """특정 행의 값 업데이트"""
```

**카운터 관리**:
```python
def rename_counter(txn, old_name: str, new_name: str):
    """테이블 이름 변경 시 카운터 키도 변경"""
    # 키: b"meta:table:{old_name}:counter" → b"meta:table:{new_name}:counter"
    
def reset_counter(txn, table_name: str):
    """테이블의 ID 카운터를 0으로 초기화"""
```

**예시 - LMDB 데이터 구조**:
```
키: b"meta:tables"
값: '["course", "instructor", "student"]'

키: b"meta:table:student:schema"
값: '{
  "table_name": "student",
  "columns": {
    "student_id": {"type": "int", "not_null": true},
    "student_name": {"type": {"base": "char", "length": 15}, "not_null": false}
  },
  "column_names": ["student_id", "student_name"],
  "primary_keys": [["student_id"]],
  "foreign_keys": [
    {
      "columns": ["student_id"],
      "ref_table": "instructor",
      "ref_columns": ["instructor_id"]
    }
  ]
}'

키: b"meta:table:student:counter"
값: b'\x00\x00\x00\x02'  # 다음 ID = 2

키: b"data:student:0"
값: '{"student_id": 1, "student_name": "Alice"}'

키: b"data:student:1"
값: '{"student_id": 2, "student_name": "Bob"}'
```

---

### 7. **config/tools/schema.py** - 스키마 데이터 구조 정의

**역할**: 스키마 객체의 구조를 정의하고 예시를 제공합니다.

#### ⭐ 중요: Primary Key 구조

**Primary Key는 `[[]]` 리스트 안의 리스트 구조입니다.**

```python
# 단일 PK
"primary_keys": [["student_id"]]  # ← 외부 리스트, 내부 리스트

# 복합 PK (여러 컬럼)
"primary_keys": [["student_id", "course_id"]]  # ← 하나의 PK 정의

# 여러 PK는 허용되지 않음 (검증 에러)
```

#### 전체 스키마 예시:
```python
schema = {
    "table_name": "student",
    
    # 컬럼 정의 (이름 → 타입 정보)
    "columns": {
        "student_id": {
            "type": "int",  # int, date, 또는 {"base": "char", "length": N}
            "not_null": True
        },
        "student_name": {
            "type": {
                "base": "char",
                "length": 15
            },
            "not_null": False
        },
        "birth_date": {
            "type": "date",
            "not_null": False
        }
    },
    
    # 컬럼 이름 순서 (유지용)
    "column_names": ["student_id", "student_name", "birth_date"],
    
    # Primary Key: [[]] 구조 (필수!)
    "primary_keys": [["student_id"]],  # ← 리스트 안의 리스트
    
    # Foreign Keys
    "foreign_keys": [
        {
            "columns": ["student_id"],
            "ref_table": "instructor",
            "ref_columns": ["instructor_id"]
        },
        {
            "columns": ["course_id"],
            "ref_table": "course",
            "ref_columns": ["course_id"]
        }
    ]
}
```

---

### 8. **config/messages/errors.py** - 사용자 정의 예외 클래스

**역할**: 데이터베이스 작업 중 발생하는 다양한 에러를 구분합니다.

**주요 예외 클래스**:

| 예외 클래스 | 발생 시나리오 |
|-----------|-----------|
| `SyntaxError` | SQL 문법 오류 |
| `DuplicateColumnDefError` | 컬럼 이름 중복 정의 |
| `DuplicatePrimaryKeyDefError` | PK 정의 중복 또는 내부 컬럼 중복 |
| `TableExistenceError` | 이미 존재하는 테이블 생성 시도 |
| `CharLengthError` | CHAR 타입 길이 <= 0 |
| `PrimaryKeyColumnDefError(colName)` | PK로 정의된 컬럼이 존재하지 않음 |
| `ForeignKeyColumnDefError(colName)` | FK로 정의된 컬럼이 존재하지 않음 |
| `ReferenceExistenceError` | FK가 참조하는 테이블/컬럼이 존재하지 않음 |
| `ReferenceTypeError` | FK-RefColumn 개수 또는 타입 불일치 |
| `NoSuchTable(commandName)` | 존재하지 않는 테이블 참조 |
| `SelectColumnResolveError(colName)` | SELECT에서 해석할 수 없는 컬럼 |
| `InsertColumnExistenceError(colName)` | INSERT 대상 컬럼이 존재하지 않음 |
| `InsertColumnNonNullableError(colName)` | NOT NULL 컬럼에 NULL 삽입 시도 |

---

### 9. **config/messages/messages.py** - 결과 및 에러 메시지 포매팅

**역할**: 쿼리 실행 결과 및 에러를 사용자 친화적인 메시지로 변환합니다.

**핵심 클래스**:
```python
class Result:
    def __init__(self, type, data=None):
        self.type = type      # "CreateTableSuccess", "DeleteResult" 등
        self.data = data      # 테이블 이름, 삭제된 행 수 등

class ExecutionResult:
    def __init__(self, result=None, error=None):
        self.result = result  # Result 객체 (성공 시)
        self.error = error    # Exception 객체 (실패 시)
    
    @property
    def is_success(self):
        return self.error is None
```

**성공 메시지 예시**:
```python
def format_success(res: Result):
    if res.type == "CreateTableSuccess":
        return f"'{res.data}' table is created"
        # 예: "'student' table is created"
    
    elif res.type == "InsertResult":
        return "1 row inserted"
    
    elif res.type == "DeleteResult":
        return f"'{res.data}' row(s) deleted"
        # 예: "'5' row(s) deleted"
    
    elif res.type == "DropSuccess":
        return f"'{res.data}' table is dropped"
```

**에러 메시지 예시**:
```python
def format_error(e: Exception):
    if isinstance(e, DuplicateColumnDefError):
        return "Create table has failed: column definition is duplicated"
    
    elif isinstance(e, CharLengthError):
        return "Char length should be over 0"
    
    elif isinstance(e, PrimaryKeyColumnDefError):
        return f"Create table has failed: cannot define non-existing column '{e.colName}' as primary key"
```

---

## 🔄 전체 실행 흐름 예시

### 예제 1: CREATE TABLE
```
입력: CREATE TABLE student (id int NOT NULL, name char(15), PRIMARY KEY (id));

1. run.py - SQL 입력 받음
2. Lark Parser - SQL 문법 검증 및 AST 생성
3. MyTransformer - AST를 다음과 같은 Python 객체로 변환:
   {
     "type": "create_table",
     "schema": {
       "table_name": "student",
       "columns": {...},
       "primary_keys": [["id"]],  # ← [[]] 구조!
       ...
     }
   }
4. QueryExecutor.execute() - "create_table" 타입 감지
5. create_table(txn, schema) 호출
6. validation_tools 함수들 실행:
   - validate_table_not_exists() ✓
   - validate_columns() ✓
   - validate_char_length() ✓
   - validate_pk_duplicates() ✓
   - validate_primary_keys() ✓
7. basic_tools 함수들 실행:
   - add_tables(txn, "student")
   - create_schema(txn, schema)
   - LMDB 저장:
     * b"meta:tables" → '["student"]'
     * b"meta:table:student:schema" → {...}
8. Result 객체 생성 및 메시지 포매팅
   - ExecutionResult(Result("CreateTableSuccess", "student"), None)
   - 메시지: "'student' table is created"
9. run.py에서 메시지 출력
```

### 예제 2: INSERT INTO
```
입력: INSERT INTO student (id, name) VALUES (1, 'Alice');

1. run.py - SQL 입력
2. MyTransformer:
   {
     "type": "insert",
     "insert_schema": {
       "table_name": "student",
       "columns": ["id", "name"],
       "values": [1, "Alice"]
     }
   }
3. QueryExecutor - "insert" 타입 감지
4. insert_into_table(txn, insert_schema) 호출
5. basic_tools.get_schema() - student 스키마 조회
6. 타입 검증:
   - id (int) = 1 ✓
   - name (char(15)) = "Alice" ✓
7. 행 추가:
   - basic_tools.add_row() 호출
   - LMDB: b"data:student:0" → '{"id": 1, "name": "Alice"}'
8. 메시지: "1 row inserted"
```

---

## 💡 주요 설계 포인트

1. **Lark 파서**: 복잡한 SQL 문법을 자동으로 파싱
2. **MyTransformer**: 파싱 트리를 Python 객체로 변환하여 처리 용이
3. **LMDB**: 빠른 키-값 저장소로 데이터 영구 저장
4. **검증 분리**: validation_tools에서 모든 제약 조건 확인
5. **메시지 포매팅**: errors와 messages를 분리하여 유지보수 용이
6. **Primary Key 구조**: `[[]]` 형태로 복합 PK 확장 가능

---

## 📌 중요 참고사항

- **Primary Keys 구조**: 항상 `[["col1"], ["col2"]]` 형태가 아니라 `[["col1", "col2"]]` 형태 (외부 리스트는 PK 정의 개수, 내부 리스트는 해당 PK 구성 컬럼)
- **LMDB 키**: 모두 바이트 문자열 (b"...") 형태로 저장
- **JSON 인코딩**: 복잡한 객체는 JSON으로 인코딩하여 저장
- **트랜잭션**: 모든 DB 작업은 txn 객체를 통해 수행
