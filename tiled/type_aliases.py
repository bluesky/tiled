from collections.abc import Callable, Coroutine, Mapping, Sequence
from types import EllipsisType
from typing import (
    Annotated,
    Any,
    TypedDict,
)

from pydantic import AfterValidator

from .utils import import_object

JSON_ITEM = str | int | float | Mapping[str, "JSON_ITEM"] | Sequence["JSON_ITEM"] | None
JSON = Mapping[str, JSON_ITEM]

AccessTags = frozenset[str]
Scopes = set[str]
Query = Any  # for now...
Filters = list[Query]
Chunks = tuple[tuple[int, ...], ...]

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
    "AccessTags",
    "AppTask",
    "EllipsisType",
    "JSON",
    "Scopes",
    "Query",
    "Filters",
    "TaskMap",
]
