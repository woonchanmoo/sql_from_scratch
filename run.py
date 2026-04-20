# run.py
# 메인 실행 스크립트: SQL 입력을 받아 파싱하고, Transformer 결과를 DB에 실행합니다.
from lark import Lark
from parser.sql_transformer import MyTransformer
from config.tools.executor import *
import lmdb

###############################################################################################
### 1. Lark
# Lark File의 상대경로 지정
LARK_PATH = "parser/grammar_skeleton.lark"

# 반복되는 Prompt 지정
PROMPT = "DB_2019-14473>"

# grammer_skeleton.lark 파일을 열고 parser를 create
with open(LARK_PATH, "r", encoding="utf-8") as f:
    sql_grammer = f.read()

sql_parser = Lark(sql_grammer, start="command", lexer="basic")
###############################################################################################

###############################################################################################
### 2. LMDB
# LMDB의 상대경로 지정
DB_PATH = "./DB/myDB.mdb"

env = lmdb.open(DB_PATH, map_size=10**9, subdir=False, lock=True, create=True)

executor = QueryExecutor(env)
###############################################################################################

# while loop을 통해 사용자의 입력을 반복적으로 받음 (exit protocol이 입력될 때 까지)
while True:

    # 중간에 exit_protocol이 작동하면 True로 변경됨
    exit_bool=False

    # 시작 prompt
    print(PROMPT, end=" ")
    buffer = ""

    # multiline 입력 받기
    while True:
        line = input()
        buffer += line + " "
        if ";" in line:
            break

    # query sequence 분리
    queries = buffer.split(';')

    for q in queries:
        q = q.strip()
        if not q:
            continue

        q += ";"  # lark grammar 맞추기

        try:
            # parse하고 MyTransformer를 통해 중간 결과를 출력하도록 함
            tree = sql_parser.parse(q)
            query = MyTransformer().transform(tree)

            # 만약 exit;이 입력되면 exit_protocol이 작동되도록
            if query == "exit":
                exit_bool = True
                break

            res = executor.execute(query)

            print_execute(res)

        # 오류가 나면 Syntax error 출력
        except Exception:
            print(f"{PROMPT} Syntax error")
            break

    # exit_protocol
    if exit_bool:
        break