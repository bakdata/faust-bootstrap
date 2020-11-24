from typing import List

from faust import App
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import TopicPartition, Consumer
from schema_registry.client import SchemaRegistryClient

from faust_bootstrap.core.streams.utils import exist_topic, parse_brokers_from_kafka_format
from .exceptions import FaustAppCleanException
from .utils import clean_tables_from_app, build_admin_client, \
    build_schema_registry_client


def clean_up_command(app: App, brokers: List[str], input_topic: str, output_topic: str, app_name: str,
                     schema_registry_url: str, error_topic: str = None, schema_registry_config: dict = None,
                     delete_output_topic: bool = False):
    schema_registry = build_schema_registry_client(schema_registry_url, schema_registry_config)
    clean_tables_from_app(app)
    client = build_admin_client(brokers)
    brokers_endpoint = parse_brokers_from_kafka_format(brokers)

    input_topic_exist = exist_topic(brokers, input_topic)

    if input_topic_exist:
        reset_offsets_from_partitions(client, ",".join(brokers_endpoint), app_name, input_topic)

    if delete_output_topic:

        output_topic_exist = exist_topic(brokers, output_topic)

        if output_topic_exist:
            delete_topic(client, output_topic)
            delete_subject(schema_registry, output_topic)

        if error_topic:

            error_topic_exist = exist_topic(brokers, error_topic)

            if error_topic_exist:
                delete_topic(client, error_topic)
                delete_subject(schema_registry, error_topic)


def delete_topic(client_admin: AdminClient, topic: str):
    fs = client_admin.delete_topics([topic])
    for topic, f in fs.items():
        f.result()


def delete_subject(client: SchemaRegistryClient, topic: str):
    subjects = client.get_subjects()
    key_subject = f"{topic}-key"
    value_subject = f"{topic}-value"
    if key_subject in subjects:
        client.delete_subject(key_subject)
    if value_subject in subjects:
        client.delete_subject(value_subject)


def get_topic(client: AdminClient, topic: str):
    try:
        list_topics = client.list_topics(timeout=20)
    except Exception as e:
        raise ConnectionError("Connection with brokers timed out :", e)
    topic_description = list_topics.topics.get(topic)
    if not topic_description:
        raise ValueError("The input topic of this app doesn't exist")
    return topic_description


def reset_offsets_from_partitions(client: AdminClient, brokers: str, app_name: str, input_topic: str):
    topic_description = get_topic(client, input_topic)
    partition_ids = [partition_metada.id for partition_metada in topic_description.partitions.values()]
    partitions = [TopicPartition(input_topic, id_partition, 0) for id_partition in partition_ids]
    consumer = Consumer({'bootstrap.servers': brokers,
                         'group.id': app_name,
                         'session.timeout.ms': 6000})
    response = consumer.commit(offsets=partitions, asynchronous=False)
    if not isinstance(response, list):
        raise FaustAppCleanException("Error while cleaning the Faust app!")
