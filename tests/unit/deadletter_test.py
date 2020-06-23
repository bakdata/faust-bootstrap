

import json
import pytest
from faust import Record

from faust_bootstrap.core.streams.utils import create_deadletter


class DummyRecord(Record):
    dummy_text: str


@pytest.fixture()
def dummy_record():
    return DummyRecord(dummy_text="This is my dummy text")


def test_deadletter_creation_from_record(dummy_record):
    fake_exception = Exception("Fake Exception")
    dead_letter = create_deadletter("Error while processing input", fake_exception, dummy_record)
    assert isinstance(dead_letter.input_value, str)
    dict_repr = json.loads(dead_letter.input_value)
    assert dummy_record == DummyRecord(**dict_repr)
    assert dead_letter.description == "Error while processing input"

def test_deadletter_creation_from_string(dummy_record):
    fake_exception = Exception("Fake Exception")
    dead_letter = create_deadletter("Error while processing input", fake_exception, "dummy-string")
    assert isinstance(dead_letter.input_value, str)
    assert dead_letter.input_value == "dummy-string"
    assert dead_letter.description == "Error while processing input"

