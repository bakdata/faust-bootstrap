from typing import Dict, List
from confluent_kafka import Consumer
from schema_registry.client.schema import AvroSchema
import sys
import time
from faust_bootstrap.core.streams.utils import build_schema_registry_client, exist_topic
from tests.integration.app import FaustAppTest
from tests.integration.utils import create_admin_client_from_metadata, setup_cluster_with_topics, \
    generate_dummy_messages, delete_topic

from pytest_docker.plugin import execute
import json
import pytest


def clean_arguments():
    sys.argv = []


@pytest.fixture()
def app():
    clean_arguments()
    return FaustAppTest()


@pytest.fixture()
def schema_registry_url(cluster_metadata):
    ip = cluster_metadata['schema_registry']['host']
    port = cluster_metadata['schema_registry']['port']
    schema_registry = f"http://{ip}:{port}"
    return schema_registry


@pytest.fixture()
def client_schema_registry(schema_registry_url):
    client = build_schema_registry_client(schema_registry_url, {})
    return client


@pytest.fixture()
def dummy_avro_schema():
    schema = {
        "type": "record",
        "namespace": "dummy",
        "name": "dummy",
        "fields": [
            {"name": "foo", "type": "string"},
            {"name": "bar", "type": "string"}
        ]
    }
    return AvroSchema(schema)


def test_command_cleanup(cluster_metadata, schema_registry_url, monkeypatch, app):
    clean_arguments()
    input_topic_name = "dummy-topic-input"
    output_topic_name = "dummy-topic-output"
    amount_of_messages = 5
    group_id = "dummy-group"
    brokers = [f"kafka://{cluster_metadata['kafka']['host']}:{cluster_metadata['kafka']['port']}"]

    set_environment_test(brokers, group_id, input_topic_name, monkeypatch, output_topic_name, schema_registry_url)

    client_admin = create_admin_client_from_metadata(cluster_metadata)

    setup_cluster_with_topics(client_admin, [input_topic_name])

    generate_dummy_messages(cluster_metadata, amount_of_messages, input_topic_name)
    read_messages_from_topic(cluster_metadata, input_topic_name, group_id, amount_of_messages, [0, 1, 2, 3, 4])

    app.start_app()
    time.sleep(5)

    read_messages_from_topic(cluster_metadata, input_topic_name, group_id, amount_of_messages, [0, 1, 2, 3, 4])

    delete_topic(client_admin, input_topic_name)


def test_command_cleanup_with_output_topic_delete(cluster_metadata, client_schema_registry, schema_registry_url,
                                                  dummy_avro_schema, monkeypatch, app):
    clean_arguments()
    input_topic_name = "dummy-topic-input"
    output_topic_name = "dummy-topic-output"
    key_subject = f"{output_topic_name}-key"
    value_subject = f"{output_topic_name}-value"
    brokers = [f"kafka://{cluster_metadata['kafka']['host']}:{cluster_metadata['kafka']['port']}"]
    amount_of_messages = 5
    group_id = "dummy-group"
    client_admin = create_admin_client_from_metadata(cluster_metadata)

    set_environment_test(brokers, group_id, input_topic_name, monkeypatch, output_topic_name, schema_registry_url,
                         "true")

    client_schema_registry.register(key_subject, dummy_avro_schema)
    client_schema_registry.register(value_subject, dummy_avro_schema)
    subjects = client_schema_registry.get_subjects()

    assert key_subject in subjects
    assert value_subject in subjects

    setup_cluster_with_topics(client_admin, [input_topic_name, output_topic_name])

    generate_dummy_messages(cluster_metadata, amount_of_messages, input_topic_name)
    read_messages_from_topic(cluster_metadata, input_topic_name, group_id, amount_of_messages, [0, 1, 2, 3, 4])

    app.start_app()
    time.sleep(5)

    read_messages_from_topic(cluster_metadata, input_topic_name, group_id, amount_of_messages, [0, 1, 2, 3, 4])

    output_topic_exist = exist_topic(brokers, output_topic_name)
    assert output_topic_exist is False

    subjects = client_schema_registry.get_subjects()
    assert key_subject not in subjects
    assert value_subject not in subjects

    delete_topic(client_admin, input_topic_name)


def set_environment_test(brokers, group_id, input_topic_name, monkeypatch, output_topic_name, schema_registry_url,
                         delete_output="false", error_topic=None):
    monkeypatch.setenv("APP_BROKERS", ",".join(brokers))
    monkeypatch.setenv("APP_INPUT_TOPICS", input_topic_name)
    monkeypatch.setenv("APP_OUTPUT_TOPIC", output_topic_name)
    monkeypatch.setenv("APP_NAME", group_id)
    monkeypatch.setenv("APP_CLEAN_UP", "true")
    monkeypatch.setenv("APP_SCHEMA_REGISTRY_URL", schema_registry_url)
    monkeypatch.setenv("APP_DELETE_OUTPUT", delete_output)
    if error_topic:
        monkeypatch.setenv("APP_ERROR_TOPIC", error_topic)


def test_command_cleanup_with_error_topic_delete(cluster_metadata, client_schema_registry, schema_registry_url,
                                                 dummy_avro_schema, monkeypatch, app):
    clean_arguments()
    input_topic_name = "dummy-topic-input"
    output_topic_name = "dummy-topic-output"
    error_topic_name = "dummy-topic-error"
    output_key_subject = f"{output_topic_name}-key"
    output_value_subject = f"{output_topic_name}-value"
    error_key_subject = f"{error_topic_name}-key"
    error_value_subject = f"{error_topic_name}-value"

    client_schema_registry.register(output_key_subject, dummy_avro_schema)
    client_schema_registry.register(output_value_subject, dummy_avro_schema)
    client_schema_registry.register(error_key_subject, dummy_avro_schema)
    client_schema_registry.register(error_value_subject, dummy_avro_schema)
    subjects = client_schema_registry.get_subjects()

    assert error_key_subject in subjects
    assert error_value_subject in subjects

    group_id = "dummy-group"
    brokers = [f"kafka://{cluster_metadata['kafka']['host']}:{cluster_metadata['kafka']['port']}"]
    client_admin = create_admin_client_from_metadata(cluster_metadata)

    set_environment_test(brokers, group_id, input_topic_name, monkeypatch, output_topic_name, schema_registry_url,
                         "true",
                         error_topic_name)


    setup_cluster_with_topics(client_admin, [input_topic_name, output_topic_name, error_topic_name])

    app.start_app()
    time.sleep(5)

    error_topic_exist = exist_topic(brokers, error_topic_name)
    assert error_topic_exist is False

    subjects = client_schema_registry.get_subjects()
    assert error_key_subject not in subjects
    assert error_value_subject not in subjects

    delete_topic(client_admin, input_topic_name)


def test_command_no_exist_topic(cluster_metadata, schema_registry_url, monkeypatch, app):
    clean_arguments()
    input_topic_name = "input-non-existant-topic"
    output_topic_name = "output-non-existant-topic"
    error_topic_name = "dummy-topic-error"
    group_id = "dummy-group"
    brokers = [f"kafka://{cluster_metadata['kafka']['host']}:{cluster_metadata['kafka']['port']}"]
    set_environment_test(brokers, group_id, input_topic_name, monkeypatch, output_topic_name, schema_registry_url,
                         "true",
                         error_topic_name)

    app.start_app()
    time.sleep(5)


def test_command_without_cleanup(cluster_metadata):
    input_topic_name = "input-dummy-topic-without-cleanup"
    amount_of_messages = 10
    group_id = "dummy-group-without-cleanup"

    client_admin = create_admin_client_from_metadata(cluster_metadata)

    setup_cluster_with_topics(client_admin, [input_topic_name])

    generate_dummy_messages(cluster_metadata, amount_of_messages, input_topic_name)

    read_messages_from_topic(cluster_metadata, input_topic_name, group_id, amount_of_messages // 2, [0, 1, 2, 3, 4])
    read_messages_from_topic(cluster_metadata, input_topic_name, group_id, amount_of_messages // 2, [5, 6, 7, 8, 9])

    delete_topic(client_admin, input_topic_name)


def read_messages_from_topic(cluster_metadata: Dict, topic: str, group_id: str, amount_of_messages_read: int,
                             assert_values: List):
    ip = cluster_metadata["kafka"]["host"]
    port = cluster_metadata["kafka"]["port"]
    conf_consumer = {
        "bootstrap.servers": f"{ip}:{port}",
        "group.id": group_id,
        "auto.offset.reset": "earliest"
    }
    c = Consumer(conf_consumer)
    c.subscribe([topic])

    for x in range(amount_of_messages_read):
        msg = c.poll(20)
        assert msg is not None, "The kafka cluster should already have messages to send"
        assert int(msg.value()) == assert_values[x], "Kafka message was not expected"
    c.close()


def get_ip_from_service(docker_service, service_name: str):
    ids = docker_service._docker_compose.execute(f"ps -q {service_name}").strip().splitlines()

    docker_info = json.loads(
        execute(f"docker inspect {ids[0].decode('utf-8')}")
    )
    networks = docker_info[0]["NetworkSettings"]["Networks"]
    for info_net in networks.values():
        return info_net["Gateway"]
