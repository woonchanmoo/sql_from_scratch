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