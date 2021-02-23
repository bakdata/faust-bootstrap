from faust_bootstrap.core.streams.cli import FaustCli
import sys


def test_cli_kafka_brokers():

    sys.argv = [
        "",
        "--brokers",
        "127.0.0.1:9092,localhost:9092",
        "--input-topics",
        "dummy-topic-input",
        "--output-topic",
        "dummy-topic-output",
        "--schema-registry-url",
        "http://127.0.0.1:8081",
    ]
    cli = FaustCli()
    cli.parse_params()
    state = cli.state
    assert state["brokers"] == ["kafka://127.0.0.1:9092", "kafka://localhost:9092"]
    assert state["input_topic_names"] == ["dummy-topic-input"]
    assert state["output_topic_name"] == "dummy-topic-output"
    assert state["schema_registry_url"] == "http://127.0.0.1:8081"
    assert state["error_topic_name"] == "dummy-error"
