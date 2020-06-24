import traceback
from typing import Callable, Union, Any
from faust import Record
from faust_bootstrap.core.streams.utils import exit_app

from .utils import create_deadletter


async def forward_error(topic_exist: Any):
    if not topic_exist:
        traceback.print_exc()
        exit_app()


def create_deadletter_message(processed: Union[Exception, Any], original: any, description: str):
    return create_deadletter(description, processed, original)


async def capture_errors(record: Record, f: Callable):
    try:
        return f(record)
    except Exception as e:
        return e
