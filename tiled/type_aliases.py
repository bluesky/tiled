import sys

from pydantic import AfterValidator

if sys.version_info < (3, 10):
    EllipsisType = type(Ellipsis)
else:
    from types import EllipsisType

from typing import (
    Annotated,
    Any,
    Callable,
    Coroutine,
    List,
    Mapping,
    Sequence,
    Set,
    TypedDict,
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


EntryPointString = Annotated[
    str,
    AfterValidator(import_object),
]


__all__ = [
    "AppTask",
    "EllipsisType",
    "JSON",
    "Scopes",
    "Query",
    "Filters",
    "TaskMap",
]
