from collections.abc import Callable, Coroutine, Mapping, Sequence
from types import EllipsisType
from typing import (
    Annotated,
    Any,
    TypedDict,
)

from pydantic import AfterValidator, PlainSerializer, SerializationInfo

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


def _serialize_entry_point(obj: Any, info: SerializationInfo) -> Any:
    # Validation replaces the "module:name" string with the object it names.
    # Dumping to Python returns that object unchanged; dumping to JSON names it.
    if not info.mode_is_json():
        return obj
    module = getattr(obj, "__module__", None)
    qualname = getattr(obj, "__qualname__", None)
    if module and qualname:
        return f"{module}:{qualname}"
    return str(obj)


EntryPointString = Annotated[
    str,
    AfterValidator(import_object),
    PlainSerializer(_serialize_entry_point, return_type=Any),
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
