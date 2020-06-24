from faust import Record

class ErrorDescription(Record):
    _schema = {
        "type": "record",
        "name": "ErrorDescription",
        "namespace": "com.bakdata.kafka",
        "fields": [
            {
                "name": "message",
                "type": [
                    "null",
                    {"type": "string", "avro.java.string": "String"}
                ]
            },
            {
                "name": "stack_trace",
                "type": [
                    "null",
                    {"type": "string", "avro.java.string": "String"}
                ]
            }
        ]
    }
    stack_trace: str
    message: str


class DeadLetter(Record):
    _schema = {
        "type": "record",
        "name": "DeadLetter",
        "namespace": "com.bakdata.kafka",
        "fields": [
            {
                "name": "input_value",
                "type": [
                    "null",
                    {"type": "string", "avro.java.string": "String"}
                ]
            },
            {
                "name": "description",
                "type": {"type": "string", "avro.java.string": "String"}
            },
            {
                "name": "cause",
                "type": {
                    "type": "record",
                    "name": "ErrorDescription",
                    "fields": [
                        {
                            "name": "message",
                            "type": [
                                "null",
                                {"type": "string", "avro.java.string": "String"}
                            ]
                        },
                        {
                            "name": "stack_trace",
                            "type": [
                                "null",
                                {"type": "string", "avro.java.string": "String"}
                            ]
                        }
                    ]
                }
            }
        ]
    }
    input_value: str
    description: str
    cause: ErrorDescription
