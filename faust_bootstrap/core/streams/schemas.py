from typing import Callable, Tuple, Any

from faust import Schema
from faust.exceptions import ValueDecodeError, KeyDecodeError
from faust.types import AppT, Message, CodecArg, V, K, ModelArg
from faust.types.core import OpenHeadersArg
from faust.types.serializers import KT, VT
import traceback
from .utils import exit_app


class ErrorHandlingSchema(Schema):

    def __init__(self, *,
                 key_type: ModelArg = None,
                 value_type: ModelArg = None,
                 key_serializer: CodecArg = None,
                 value_serializer: CodecArg = None,
                 allow_empty: bool = None) -> None:

        super(ErrorHandlingSchema, self).__init__(key_type=key_type, value_type=value_type,
                                                  key_serializer=key_serializer,
                                                  value_serializer=value_serializer, allow_empty=allow_empty)

    def loads_value(self, app: AppT, message: Message, *, loads: Callable = None, serializer: CodecArg = None) -> VT:
        try:
            return super().loads_value(app, message, loads=loads, serializer=serializer)
        except ValueDecodeError:
            traceback.print_exc()
            exit_app()

    def dumps_key(self, app: AppT, key: K, *, serializer: CodecArg = None, headers: OpenHeadersArg) -> Tuple[
        Any, OpenHeadersArg]:
        try:
            return super().dumps_key(app, key, serializer=serializer, headers=headers)
        except KeyDecodeError:
            traceback.print_exc()
            exit_app()

    def dumps_value(self, app: AppT, value: V, *, serializer: CodecArg = None, headers: OpenHeadersArg) -> Tuple[
        Any, OpenHeadersArg]:
        try:
            return super().dumps_value(app, value, serializer=serializer, headers=headers)
        except ValueDecodeError:
            traceback.print_exc()
            exit_app()

    def loads_key(self, app: AppT, message: Message, *, loads: Callable = None, serializer: CodecArg = None) -> KT:
        try:
            return super().loads_key(app, message, loads=loads, serializer=serializer)
        except KeyDecodeError:
            traceback.print_exc()
            exit_app()
