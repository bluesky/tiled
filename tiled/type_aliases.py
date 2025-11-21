import sys

if sys.version_info < (3, 10):
    EllipsisType = type(Ellipsis)
else:
    from types import EllipsisType

from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Callable,
    Coroutine,
    List,
    Mapping,
    Sequence,
    Set,
    TypedDict,
    TypeVar,
    Union,
)

from .utils import import_object

JSON_ITEM = Union[
    str, int, float, Mapping[str, "JSON_ITEM"], Sequence["JSON_ITEM"], None
]
JSON = Mapping[str, JSON_ITEM]

Scopes = Set[str]
Query = Any  # for now...
Filters = List[Query]
AccessBlob = Mapping[str, Any]
AccessTags = Set[str]

AppTask = Callable[[], Coroutine[None, None, Any]]
"""Async function to be run as part of the app's lifecycle"""


class TaskMap(TypedDict):
    background: list[AppTask]
    startup: list[AppTask]
    shutdown: list[AppTask]


if TYPE_CHECKING:
    # Let type checking treat this as just the underlying type
    T = TypeVar("T")
    EntryPointString = Annotated[T, ...]
else:
    from pydantic import GetCoreSchemaHandler, GetJsonSchemaHandler
    from pydantic.json_schema import JsonSchemaValue
    from pydantic.types import AnyType
    from pydantic_core import CoreSchema, core_schema

    class EntryPointString:
        """
        Version of Pydantic's ImportString that supports importing fields of
        imported items, not just top level attributes

        A string such as `path.to.module:Type.field` is equivalent to

        ```
        from path.to.module import type
        return type.field
        ```

        """

        @classmethod
        def __class_getitem__(cls, item: AnyType):
            return Annotated[item, cls()]

        @classmethod
        def __get_pydantic_core_schema__(
            cls, source: type[Any], handler: GetCoreSchemaHandler
        ) -> core_schema.CoreSchema:
            return core_schema.no_info_plain_validator_function(function=import_object)

        @classmethod
        def __get_pydantic_json_schema__(
            cls, cs: CoreSchema, handler: GetJsonSchemaHandler
        ) -> JsonSchemaValue:
            return handler(core_schema.str_schema())

        def __repr__(self) -> str:
            return "EntryPointString"


__all__ = [
    "AppTask",
    "EllipsisType",
    "JSON",
    "Scopes",
    "Query",
    "Filters",
    "TaskMap",
]
