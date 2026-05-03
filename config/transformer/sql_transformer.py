# 반복되는 Prompt 지정
from lark import Transformer, Token
from config.tools.query_tools import *

PROMPT = "DB_2019-14473>"

# Lark에서 파싱된 트리를 Python 객체로 변환하는 Transformer 클래스
class MyTransformer(Transformer):
    def __init__(self):
        self.txn = None

    # 명령어 루트 노드를 단순히 하위 노드로 전달
    def command(self, items):
        return items[0]
    
    def query_list(self, items):
        return items[0]
    
    def query(self, items):
        return items[0]

    # -------------------------
    # 기본 요소
    # -------------------------
    def table_name(self, items):
        # 테이블 이름을 소문자로 일관되게 변환
        return str(items[0]).lower()

    def column_name(self, items):
        # 컬럼 이름도 소문자로 변환하여 비교를 쉽게 함
        return str(items[0]).lower()

    def data_type(self, items):
        # 데이터 타입 정보 변환, CHAR 타입은 길이까지 함께 저장
        base = str(items[0]).lower()

        if base == "char":
            return {"base": "char", "length": int(items[2])}
        return base

    # -------------------------
    # column 정의
    # -------------------------
    def column_definition(self, items):
        # CREATE TABLE 내 컬럼 정의를 파싱하여 이름, 데이터 타입, NOT NULL 여부를 반환
        col_name = items[0]
        data_type = items[1]

        is_not_null = False
        if str(items[3]).lower() == "null":
            is_not_null = True

        return {
            "type": "column",
            "name": col_name,
            "data_type": data_type,
            "not_null": is_not_null
        }
        
    def column_name_list(self, items):
        # 괄호 토큰을 제외하고 실제 컬럼 이름 목록만 추출
        result = []
        for item in items:
            if isinstance(item, Token):
                continue  # LP, RP 제거
            result.append(item)
        return result

    # -------------------------
    # primary key
    # -------------------------
    def table_constraint_definition(self, items):
        return items[0]

    def primary_key_constraint(self, items):
        # PRIMARY KEY 제약 조건은 PK 컬럼 리스트만 추출
        return {
            "type": "pk",
            "columns": items[-1]
        }

    # -------------------------
    # foreign key
    # -------------------------
    def referential_constraint(self, items):
        # FOREIGN KEY 제약 조건 정보를 딕셔너리로 생성
        return {
            "type": "fk",
            "columns": items[2],
            "ref_table": items[4],
            "ref_columns": items[5]
        }

    # -------------------------
    # table element wrapper
    # -------------------------
    def table_element(self, items):
        return items[0]

    def table_element_list(self, items):
        return [item for item in items if not hasattr(item, 'type')]

    # -------------------------
    # INSERT & Values
    # -------------------------
    
    def value_type(self, items):
        # INSERT VALUES 내부 요소를 Python 값으로 변환
        token = items[0]
        if token.type == 'STR':
            # 따옴표를 제거한 문자열 반환
            return token.value.strip('"').strip("'")
        elif token.type == 'INT':
            return int(token.value)
        elif token.type == 'NULL':
            return None
        return token.value
    
    def value_list(self, items):
        # 괄호 토큰을 제외하고 실제 값 목록만 반환
        return [item for item in items if not isinstance(item, Token)]

    # -------------------------
    # SELECT
    # -------------------------

    def select_list(self, items):
        # SELECT 리스트를 변환, '*'는 별도 표기로 처리
        if len(items) == 0:
            return ["*"]
        return items

    def table_expression(self, items):
        # FROM, WHERE 등 SELECT 절을 하나의 구조로 묶음
        res = {
            "from_clause": items[0]
        }
        
        if len(items) > 1 and items[1] is not None:
            res["where_clause"] = items[1]
            
        # 추가 절이 있으면 여기에 계속 확장 가능
        return res

    def from_clause(self, items):
        # items[1]이 table_reference_list입니다.
        return items[1]

    def table_reference_list(self, items):
        # 여러 테이블이 올 수 있으므로 리스트로 반환
        return items

    def referred_table(self, items):
        # items[0]: table_name, items[1]: alias(None) (as ...)
        return items[0]
    
    # -------------------------
    # RENAME
    # -------------------------
    def rename_expression(self, items):
        return {
            "old_name": items[0],
            "new_name": items[2]
        }

    ###
    # -------------------------
    # CREATE TABLE
    # -------------------------
    ###
    def create_table_query(self, items):
        # CREATE TABLE 쿼리를 스키마 표현으로 변환
        table_name = items[2]
        elements = items[3]

        columns = {}
        column_names = []
        primary_keys = []
        foreign_keys = []

        for el in elements:

            if el["type"] == "column":
                col_name = el["name"]
                column_names.append(col_name)

                columns[col_name] = {
                    "type": el["data_type"],
                    "not_null": el.get("not_null", False)
                }

            elif el["type"] == "pk":
                primary_keys.append(el["columns"])

                # Primary Key는 자동으로 NOT NULL 처리
                for pk_col in el["columns"]:
                    if pk_col in columns:
                        columns[pk_col]["not_null"] = True

            elif el["type"] == "fk":
                foreign_keys.append({
                    "columns": el["columns"],
                    "ref_table": el["ref_table"],
                    "ref_columns": el["ref_columns"]
                })

        schema = {
            "table_name": table_name,
            "columns": columns,
            "column_names": column_names,
            "primary_keys": primary_keys,
            "foreign_keys": foreign_keys
        }

        return {
            "type": "create_table",
            "schema": schema
        }

    def drop_table_query(self, items):
        # print(f"{PROMPT} 'DROP TABLE' requested")

        table_name = items[2]

        return {
            "type": "drop_table",
            "table_name": table_name
        }
        
    def explain_query(self, items):
        # print(f"{PROMPT} 'EXPLAIN' requested")
        table_name = items[1]

        return {
            "type": "explain",
            "table_name": table_name
        }

    def describe_query(self, items):
        # print(f"{PROMPT} 'DESCRIBE' requested")
        table_name = items[1]

        return {
            "type": "describe",
            "table_name": table_name
        }

    def desc_query(self, items):
        # print(f"{PROMPT} 'DESC' requested")
        table_name = items[1]

        return {
            "type": "desc",
            "table_name": table_name
        }

    def insert_query(self, items):
        # INSERT 쿼리를 파싱하여 테이블명, 컬럼 리스트, 값 리스트를 반환
        # ### For debugging
        for i, item in enumerate(items):
            print(f"ITEM{i}: {item}")

        # ITEM0: 'insert', ITEM1: 'into', ITEM2: 'nameage', ITEM3: col_names(None), ITEM4: 'values', ITEM5: value_list

        table_name = items[2]
        column_names = items[3] # 명시된 컬럼이 없으면 None
        values = items[5]       # 위에서 처리된 순수 값 리스트

        insert_schema = {
            "table_name": table_name,
            "column_names": column_names,
            "values": values
        }
        
        return {
            "type": "insert",
            "insert_schema": insert_schema
        }

    def delete_query(self, items):
        print(f"{PROMPT} 'DELETE' requested")

        # ### For debugging
        for i, item in enumerate(items):
            print(f"ITEM{i}: {item}")

        table_name = items[2]
        where_clause = items[3] if len(items) > 3 else None

        return {
            "type": "delete",
            "delete_schema": {
                "table_name": table_name,
                "where_clause": where_clause
            }
        }

    def comparable_value(self, items):
        # INT | STR | DATE tokens
        # Return with type marker so comp_operand can distinguish from column_name
        token = items[0]
        if token.type == 'STR':
            return {
                "_literal_type": "str",
                "value": token.value.strip('"').strip("'")
            }
        elif token.type == 'INT':
            return {
                "_literal_type": "int",
                "value": int(token.value)
            }
        elif token.type == 'DATE':
            return {
                "_literal_type": "date",
                "value": token.value
            }
        else:
            return {
                "_literal_type": "unknown",
                "value": token.value
            }

    def comp_operand(self, items):
        # comp_operand can be comparable_value or [table_name "."] column_name
        # Always return consistent dictionary format for easier downstream processing
        if len(items) == 1:
            item = items[0]
            # Distinguish: comparable_value returns dict with "_literal_type"
            if isinstance(item, dict) and "_literal_type" in item:
                # It's a literal value (int, str, or date)
                return {
                    "type": "value",
                    "value": item["value"]
                }
            else:
                # It's a column_name (string, lowercase)
                return {
                    "type": "column",
                    "table": None,
                    "column": item
                }
        elif len(items) == 2:
            # [table_name, column_name]
            return {
                "type": "column",
                "table": items[0],
                "column": items[1]
            }
        else:
            # Shouldn't reach here
            return {
                "type": "unknown",
                "value": items
            }

    def comp_op(self, items):
        return str(items[0]).lower()

    def _negate_operator(self, op):
        """Negate comparison operator for NOT optimization"""
        negations = {
            '=': '!=',
            '!=': '=',
            '>': '<=',
            '>=': '<',
            '<': '>=',
            '<=': '>'
        }
        return negations.get(op, op)

    def comparison_predicate(self, items):
        return {
            "type": "comparison",
            "operator": items[1],
            "left": items[0],
            "right": items[2]
        }

    def null_operation(self, items):
        # IS [NOT] NULL
        has_not = any(
            isinstance(item, Token) and item.type == 'NOT'
            for item in items
        )
        return {
            "type": "null_check",
            "not_null": has_not
        }

    def null_predicate(self, items):
        # [table_name "."] column_name null_operation
        if len(items) == 2:
            column_ref = {
                "type": "column",
                "table": None,
                "column": items[0]
            }
            null_op = items[1]
        else:
            column_ref = {
                "type": "column",
                "table": items[0],
                "column": items[1]
            }
            null_op = items[2]
        return {
            "type": "null_predicate",
            "column": column_ref,
            "is_not_null": null_op.get("not_null", False)
        }

    def predicate(self, items):
        return items[0]

    def boolean_test(self, items):
        return items[0]

    def boolean_factor(self, items):
        
        # NOT이 없으면 (default) 그대로 predicate를 return
        if not items[0]:
            return items[1]
        # NOT이 있으면 operand의 operator를 반전시켜 NOT을 제거
        operand = items[-1]
        if operand["type"] == "comparison":
            return {
                "type": "comparison",
                "operator": self._negate_operator(operand["operator"]),
                "left": operand["left"],
                "right": operand["right"]
            }
        elif operand["type"] == "null_predicate":
            return {
                "type": "null_predicate",
                "column": operand["column"],
                "is_not_null": not operand["is_not_null"]
            }
        else:
            # 다른 타입은 NOT으로 감싸기
            return {
                "type": "not",
                "operand": operand
            }

    def boolean_term(self, items):
        terms = [item for item in items if not isinstance(item, Token)]
        if len(terms) == 1:
            return terms[0]
        return {
            "type": "and",
            "operands": terms
        }

    def boolean_expr(self, items):
        terms = [item for item in items if not isinstance(item, Token)]
        if len(terms) == 1:
            return terms[0]
        return {
            "type": "or",
            "operands": terms
        }

    def parenthesized_boolean_expr(self, items):
        return items[1]

    def where_clause(self, items):
        return items[1] if len(items) > 1 else None

    def select_query(self, items):
        # SELECT 쿼리를 AST 형식으로 변환
        # print(f"{PROMPT} 'SELECT' requested")

        # ### For debugging
        # for i, item in enumerate(items):
        #     print(f"ITEM{i}: {item}")
           
        # items[0]: 'select', items[1]: select_list, items[2]: table_expression

        select_list = items[1]
        from_list = items[2].get("from_clause", [])

        select_schema = {
            "select_list": select_list,
            "from_list": from_list
        }

        return {
            "type": "select",
            "select_schema": select_schema
        }

    def show_tables_query(self, items):
        # print(f"{PROMPT} 'SHOW TABLES' requested")

        return {"type": "show_tables"}

    def update_query(self, items):
        print(f"{PROMPT} 'UPDATE' requested")
        # ### For debugging
        # for i, item in enumerate(items):
        #     print(f"ITEM{i}: {item}")

    def rename_table_query(self, items):
        # RENAME TABLE 절을 파싱하여 변경 전/후 이름을 반환
        # print(f"{PROMPT} 'RENAME TABLE' requested")

        # ### For debugging
        # for i, item in enumerate(items):
        #     print(f"ITEM{i}: {item}")   

        # the schema is from rename_expression(self, items)
        rename_schema = items[2]

        return {
            "type": "rename",
            "rename_schema": rename_schema
        }

    def truncate_table_query(self, items):
        # TRUNCATE TABLE 쿼리를 처리
        # print(f"{PROMPT} 'TRUNCATE TABLE' requested")

        table_name = items[2]

        return {
            "type": "truncate_table",
            "table_name": table_name
        }

    def EXIT(self, token):
        return "exit"
