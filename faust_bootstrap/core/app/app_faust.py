import faust
from faust import Record, ChannelT
from faust.types import CodecT
from abc import ABC, abstractmethod
import importlib
import os
import sys
import pkgutil
from typing import Any, Iterable, Union, List, Callable
import logging


from faust.types.agents import AgentFun
from faust_avro_serializer import FaustAvroSerializer

from faust_bootstrap.core.streams.commands import clean_up_command
from faust_bootstrap.core.streams.schemas import ErrorHandlingSchema
from faust_bootstrap.core.streams.utils import get_streams_config, build_faust_config, build_schema_registry_client, \
    records_to_dict, \
    exist_topic

from faust_bootstrap.core.streams.cli import FaustCli


class FaustApplication(ABC):
    PREFIX = "STREAMS_"
    brokers: str
    input_topic_names: List[str]
    output_topic_name: str
    error_topic_name: str
    streams_config: str
    schema_registry_url: str
    delete_output: bool = False
    clean_up: bool = False
    app: faust.App

    def __init__(self):
        self.__paths_to_discover = []
        self.__monkey_patch_record()
        self._cli = FaustCli()

    def create_agent(self, agent_function: AgentFun[Callable], channel: Union[str, ChannelT] = None, **kwargs):
        self.app.agent(channel, **kwargs)(agent_function)

    def register_parameter(self, name: str, required: bool, help: str, type: Any, **kwargs):
        self._cli.add_option(name, required, help, type, **kwargs)

    def _register_parameters(self):
        ...

    def discover_in_path(self, path):
        self.__paths_to_discover.append(path)

    def __discover_in_paths(cls, path):
        modules = pkgutil.walk_packages(path=[path])
        for module in modules:
            logging.debug(f"Importing path {path}")
            if module.name not in sys.modules:
                module_spec = module.module_finder.find_spec(module.name)
                module_data = importlib.util.module_from_spec(module_spec)
                sys.modules[module.name] = module_data
                module_spec.loader.exec_module(module_data)
                logging.debug(f"Path {path} imported!")


    @abstractmethod
    def build_topology(self):
        ...

    @abstractmethod
    def setup_topics(self):
        ...

    def __load_app(self):
        self._register_parameters()
        self._read_cli_params()
        setting = self._generate_configuration()
        self.app = self._create_app_from_config(setting)
        self.setup_topics()
        self.build_topology()

        self.__load_dependencies_to_discover()

    def __load_dependencies_to_discover(self):
        for path in self.__paths_to_discover:
            self.__discover_in_paths(path)

    @abstractmethod
    def get_unique_app_id(self):
        ...

    def _read_cli_params(self):
        self._cli.parse_params()
        state = self._cli.state
        self.__dict__ = {**self.__dict__, **state}

    def get_topic_from_schema(self, topic: Union[str, List[str]], schema):
        if isinstance(topic, list):
            return self.app.topic(*topic, schema=schema)
        else:
            return self.app.topic(topic, schema=schema)

    def get_topic(self, topic: Union[str, Iterable[str]],
                  key_serializer: CodecT,
                  value_serializer: CodecT,
                  key_type: Any,
                  value_type: Any):
        schema = self.create_schema_from(key_serializer=key_serializer, value_serializer=value_serializer,
                                         value_type=value_type, key_type=key_type)
        return self.get_topic_from_schema(topic, schema)

    def create_avro_serde(self, topic: str, is_key: bool = False) -> CodecT:
        conf_streams = self._generate_streams_config()
        client = build_schema_registry_client(self.schema_registry_url, conf_streams)
        return FaustAvroSerializer(client, topic, is_key)

    def create_schema_from(self, key_serializer: CodecT = None, value_serializer: CodecT = None, key_type: Any = None,
                           value_type: Any = None):
        return ErrorHandlingSchema(key_serializer=key_serializer, value_serializer=value_serializer, key_type=key_type,
                                   value_type=value_type)

    def _generate_streams_config(self) -> dict:
        streams_config = get_streams_config(self.PREFIX, os.environ)
        return streams_config

    def _generate_configuration(self) -> faust.Settings:
        streams_config = self._generate_streams_config()
        faust_configuration = build_faust_config(self.brokers, streams_config)
        settings = faust.Settings(id=self.get_unique_app_id(), **faust_configuration)
        return settings

    def _create_app_from_config(self, config: faust.Settings):
        app = faust.App(id=self.get_unique_app_id())
        app.conf = config
        return app

    def __monkey_patch_record(self):
        def deep_self_repr(self):
            payload = self.asdict()
            options = self._options
            if options.include_metadata:
                payload['__faust'] = {'ns': options.namespace}
            payload = records_to_dict(payload)
            return payload

        Record.to_representation = deep_self_repr

    def __verify_topology(self):
        for input_topic in self.input_topic_names:

            input_topic_exists = exist_topic(self.brokers, input_topic)

            if not input_topic_exists:
                raise ValueError(f"The input topic {input_topic} doesn't exist")
        output_topic_exists = exist_topic(self.brokers, self.output_topic_name)
        if not output_topic_exists:
            raise ValueError(f"The output topic {self.output_topic_name} doesn't exist")
        if self.error_topic_name:
            error_topic_exists = exist_topic(self.brokers, self.output_topic_name)
            if not error_topic_exists:
                raise ValueError(f"The error topic {self.error_topic_name} doesn't exist")

    def __clean_and_exit(self):
        for input_topic in self.input_topic_names:
            clean_up_command(app=self.app,
                             brokers=self.brokers,
                             input_topic=input_topic,
                             output_topic=self.output_topic_name,
                             error_topic=self.error_topic_name, app_name=self.get_unique_app_id(),
                             schema_registry_url=self.schema_registry_url,
                             schema_registry_config=self._generate_streams_config(),
                             delete_output_topic=self.delete_output)

        return True

    def start_app(self):
        self.__load_app()
        if self.clean_up:
            self.__clean_and_exit()
        else:
            self.__verify_topology()
            self.__start_worker()

    def __start_worker(self):
        sys.argv = ["", "worker", "-l", "info"]
        self.app.main()
