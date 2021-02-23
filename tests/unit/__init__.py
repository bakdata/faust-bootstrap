import os

os.environ["APP_BROKERS"] = "kafka://127.0.0.1"
os.environ["APP_INPUT_TOPICS"] = "dummy-input"
os.environ["APP_OUTPUT_TOPIC"] = "dummy-output"
os.environ["APP_ERROR_TOPIC"] = "dummy-error"
os.environ["APP_SCHEMA_REGISTRY_URL"] = "http://127.0.0.1:8081"
os.environ["APP_NAME"] = "dummy-name"