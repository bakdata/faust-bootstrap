from typing import Any

import click


class FaustCli:
    def __init__(self):
        self._base_function = self._default_cli_params()

    def _default_cli_params(self):
        @click.command()
        @click.option(
            "--brokers",
            help="Brokers list with kafka protocol e.g kafka://localhost:9092",
            required=True,
            type=str,
        )
        @click.option(
            "--input-topics", help="Input topics as list", required=True, type=str
        )
        @click.option("--output-topic", required=True, type=str)
        @click.option(
            "--error-topic", help="Topic to send deadletter", required=False, type=str
        )
        @click.option("--schema-registry-url", required=True, type=str)
        @click.option("--streams-config", required=False, type=str)
        @click.option("--clean-up", required=False, type=bool, default=False)
        @click.option("--delete-output", required=False, type=bool, default=False)
        def load_cli(*args, **kwargs):
            kwargs = self._remap_properties(kwargs)
            self._state = kwargs

        return load_cli

    def _remap_properties(self, properties):
        properties["input_topic_names"] = properties["input_topics"].split(",")
        properties["output_topic_name"] = properties["output_topic"]
        properties["error_topic_name"] = properties["error_topic"]
        properties["brokers"] = self._pre_process_brokers(properties["brokers"])
        del properties["input_topics"]
        del properties["output_topic"]
        del properties["error_topic"]

        return properties

    def _pre_process_brokers(self, brokers):
        list_of_brokers = brokers.split(",")
        append_prefix_if_necessary = (
            lambda x: f"kafka://{x}" if not x.startswith("kafka://") else x
        )
        return list(map(append_prefix_if_necessary, list_of_brokers))

    def add_option(
        self, option_name: str, required: bool, help: str, type: Any, **kwargs
    ):
        options = dict(kwargs)
        options["required"] = required
        options["type"] = type
        options["help"] = help
        click.option(option_name, **options)(self._base_function)

    def parse_params(self):
        self._base_function(auto_envvar_prefix="APP", standalone_mode=False)

    @property
    def state(self):
        return self._state
