import ssl
from typing import List

import certifi
import faust
from faust import Record
from faust_avro_serializer import FaustAvroSerializer
from schema_registry.client import SchemaRegistryClient
from confluent_kafka.admin import AdminClient
import os
import traceback
import json

from .models import DeadLetter, ErrorDescription


def parse_comma_separated_params(params: str):
    if not params:
        return {}
    variables = params.split(",")
    return {key: value for key, value in
            [(variable[0:variable.find("=")], variable[variable.find("=") + 1:]) for variable in variables]}


def parse_streams_config(config: dict):
    AUTH_CREDENTIALS_KEY = "sasl.jaas.config"
    REPLICATION_FACTOR = "replication.factor"

    if AUTH_CREDENTIALS_KEY in config:
        username = config[AUTH_CREDENTIALS_KEY].split('username="')[1].split('"')[0]
        password = config[AUTH_CREDENTIALS_KEY].split('password="')[1].split('"')[0]
        config["SASL_AUTH"] = {
            "username": username,
            "password": password
        }

    config[REPLICATION_FACTOR] = int(config[REPLICATION_FACTOR]) if REPLICATION_FACTOR in config else 1

    return config


def build_schema_registry_client(url_schema, config: dict):
    if "schema.registry.basic.auth.user.info" in config:
        config_schema = {
            "url": url_schema,
            "ssl.ca.location": None,
            "ssl.certificate.location": None,
            "ssl.key.location": None,
            "basic.auth.user.info": config["schema.registry.basic.auth.user.info"],
            "basic.auth.credentials.source": "USER_INFO"
        }
        client_schema = SchemaRegistryClient(url=config_schema)
        return client_schema

    return SchemaRegistryClient(url_schema)


def build_faust_config(brokers: str, config: dict):
    ssl_context = ssl.create_default_context()
    ssl_context.load_verify_locations(cafile=certifi.where())
    faust_config = {}
    faust_config.update(broker=brokers)
    faust_config.update(broker_session_timeout=20)
    faust_config.update(topic_replication_factor=config["replication.factor"])
    if "SASL_AUTH" in config:
        faust_config.update(broker_credentials=faust.SASLCredentials(
            username=config["SASL_AUTH"]["username"],
            password=config["SASL_AUTH"]["password"],
            ssl_context=ssl.create_default_context(),
            mechanism="PLAIN"
        ))

    return faust_config


def build_avro_serializer(output_topic: str, client: SchemaRegistryClient, is_key: bool = False):
    faust_avro_serializer = FaustAvroSerializer(client, output_topic, is_key)
    return faust_avro_serializer


def should_use(flag: str):
    return flag == "true"


def build_admin_client(brokers: List[str]) -> AdminClient:
    brokers_kafka_format = parse_brokers_from_kafka_format(brokers)
    client = AdminClient({"bootstrap.servers": ",".join(brokers_kafka_format)})
    return client


def parse_brokers_from_kafka_format(brokers_kafka: List[str]) -> List[str]:
    return [broker.split("kafka://")[1] for broker in brokers_kafka]


def clean_tables_from_app(app: faust.App):
    for table in app.tables.values():
        table.reset_state()


def exist_topic(brokers: List[str], topic: str):
    client_admin = build_admin_client(brokers)
    try:
        cluster_metadata = client_admin.list_topics(timeout=20)
    except Exception as e:
        raise ConnectionError("Connection with brokers timed out :", e) from e
    if topic in cluster_metadata.topics:
        return True

    return False


def create_deadletter(description: str, reason: Exception, input_value: any = None):
    stack_trace = "".join(traceback.TracebackException.from_exception(reason).format())
    error_description = ErrorDescription(message=str(reason),
                                         stack_trace=stack_trace)
    if isinstance(input_value, Record):
        input_value = json.dumps(input_value.to_representation())
    else:
        input_value = str(input_value)

    deadletter = DeadLetter(description=description, cause=error_description, input_value=input_value)
    return deadletter


def exit_app():
    os._exit(-1)


def load_configuration(prefix: str, env: dict):
    defined_config = parse_comma_separated_params(env.get("APP_STREAMS_CONFIG", ""))
    dynamic_config = {}
    prefix_lower = prefix.lower()
    for key, value in env.items():
        key_lower = key.lower()
        if key_lower.startswith(prefix_lower):
            key_without_prefix = key_lower[len(prefix_lower):]
            dynamic_config[key_without_prefix.replace('_', '.')] = value

    return {**defined_config, **dynamic_config}


def get_streams_config(prefix: str, env: dict):
    comma_stream_params = load_configuration(prefix, env)
    return parse_streams_config(comma_stream_params)


def records_to_dict(record):
    if isinstance(record, Record):
        for key in vars(record).keys():
            value = getattr(record, key)
            if isinstance(value, Record):
                setattr(record, records_to_dict(value.asdict()))
            if isinstance(value, list):
                records_to_dict(value)
            if isinstance(value, dict):
                records_to_dict(value)

    if isinstance(record, dict):
        for key in record.keys():
            value = record[key]
            if isinstance(value, Record):
                record[key] = records_to_dict(value.asdict())
            if isinstance(value, list):
                records_to_dict(value)
            if isinstance(value, dict):
                records_to_dict(value)

    if isinstance(record, list):
        for index, item in enumerate(record):
            if isinstance(item, Record):
                record[index] = records_to_dict(item.asdict())
            if isinstance(item, list):
                records_to_dict(item)
            if isinstance(item, dict):
                records_to_dict(item)
    return record
