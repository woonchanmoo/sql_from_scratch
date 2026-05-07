# schema.py
# 예시 테이블 스키마를 보여주는 샘플 데이터 구조입니다.
schema = {
    "table_name": "student",
    "columns": {
        "student_id": {
            "type": "int",
            "not_null": True
        },
        "student_name": {
            "type": {
                "base": "char",
                "length": 15
            },
            "not_null": False
        }
    },
    "column_names": ["student_id", "student_name"],
    "primary_keys": [["student_id"]],
    "foreign_keys": [
        {
            "columns": [
                "student_id"
            ],
            "ref_table": "student",
            "ref_columns": [
                "student_id"
            ]
        },
        {
            "columns": [
                "instructor_id"
            ],
            "ref_table": "instructor",
            "ref_columns": [
                "ID"
            ]
        }
    ]
}

select_schema = {
    "select_list": [
        {"type": "column",    "table": "s",       "column": "name",  "alias": "sname"},
        {"type": "aggregate", "func":  "min",      "table": "e",      "column": "score", "alias": "avg_score"}
    ],
    "from_list":   ["student"],                        # 실제 테이블 이름 (alias 아님)
    "join_list": [
        {
            "_clause": "join",
            "table":   "enroll",
            "left":    {"table": "s",      "column": "id"},
            "right":   {"table": "enroll", "column": "sid"}
        }
    ],
    "where_clause": {
        "type": "comparison", "operator": ">",
        "left":  {"type": "column", "table": "s", "column": "age"},
        "right": {"type": "value",  "value": 18}
    },
    "group_by":  {"_clause": "group_by",  "table": None, "column": "dept"},
    "order_by":  {"_clause": "order_by",  "table": None, "column": "avg_score", "direction": "asc"},
    "limit":  5,
    "offset": 0
}

delete_schema = {
    "table_name": "nameage",
    "where_clause": {
        'type': 'and',
        'operands': [
            {
                'type': 'comparison',
                'operator': '!=',
                'left': {'type': 'column', 'table': None, 'column': 'age'},
                'right': {'type': 'value', 'value': 23}
            },
            {
                'type': 'null_predicate',
                'column': {'type': 'column', 'table': None, 'column': 'name'},
                'is_not_null': False
            }
        ]
    }
}