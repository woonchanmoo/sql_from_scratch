tool 추가하는 순서:
1. sql_transformer -> query 생성
2. executor -> executor.execute(query)로 입력받음
3. query_tool를 부른다
4. validation_tools를 부른다.
5. error나 messages를 추가한다.