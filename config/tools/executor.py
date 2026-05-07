from config.tools.query_tools import *
from config.messages.messages import *
PROMPT = "DB_2019-14473> "

# MyTransformer로 부터 반환된 파싱 결과를 실제 DB 실행으로 매핑하는 클래스
class QueryExecutor:
    def __init__(self, env):
        self.env = env

    def execute(self, query):
        """Transformer가 반환한 query dict를 query type에 따라 적절한 핸들러로 위임한다."""
        with self.env.begin(write=True) as txn:
            qtype = str(query["type"]).strip()

            if qtype == "create_table":
                return create_table(txn, query["schema"])

            elif qtype == "drop_table":
                return drop_table(txn, query["table_name"])

            # EXPLAIN / DESCRIBE / DESC: 테이블 스키마 출력
            elif qtype == "explain":
                return explain_table(txn, query["table_name"], "explain")

            elif qtype == "describe":
                return explain_table(txn, query["table_name"], "describe")

            elif qtype == "desc":
                return explain_table(txn, query["table_name"], "desc")

            # SHOW TABLES: 전체 테이블 목록 출력
            elif qtype == "show_tables":
                return show_tables(txn)

            elif qtype == "insert":
                return insert_into_table(txn, query["insert_schema"])

            elif qtype == "rename":
                return rename_table(txn, query["rename_schema"])

            elif qtype == "truncate_table":
                return truncate_table(txn, query["table_name"])

            elif qtype == "delete":
                return delete_from_table(txn, query["delete_schema"])

            elif qtype == "select":
                return select_table(txn, query["select_schema"])
            # else:
            #     raise Exception(f"Unknown query type: {qtype}")


def print_execute(res: ExecutionResult):
    # 1. 실행이 성공했을 때
    if res.is_success:
        # 데이터가 존재하는 경우에만 Prompt와 결과를 출력
        if res.result and res.result.data:
            print(PROMPT + format_success(res.result))
        # 데이터가 None이거나 비어있으면 아무것도 하지 않음 (pass)
        else:
            pass
    else:
        print(PROMPT + format_error(res.error))