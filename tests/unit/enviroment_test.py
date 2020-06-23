

from faust_bootstrap.core.streams.utils import load_configuration


def test_read_environment():
    env_only_app_streams = {
        "APP_STREAMS_CONFIG": "foo=hello,bar=something,foo.bar=anything"
    }

    env_only_app_streams_expected = {
        "foo": "hello",
        "bar": "something",
        "foo.bar": "anything"
    }
    config = load_configuration("STREAMS_", env_only_app_streams)

    assert config == env_only_app_streams_expected, "The app streams should be readable"

    env_only_dynamic = {
        "STREAMS_FOO_BAR_X": "somethinghere",
        "STREAMS_FOOBAR": "dummy",
        "STREAMS_BARFOO": "dummy_bar"
    }

    env_only_dynamic_expected = {
        "foo.bar.x": "somethinghere",
        "foobar": "dummy",
        "barfoo": "dummy_bar"
    }

    config = load_configuration("STREAMS_", env_only_dynamic)

    assert config == env_only_dynamic_expected

    config = load_configuration("STREAMS_", {**env_only_dynamic, **env_only_app_streams})

    assert config == {**env_only_app_streams_expected, **env_only_dynamic_expected}

    config = load_configuration("STREAMS_", {})

    assert len(config) == 0 and isinstance(config, dict)
