import sys

from pydantic import AfterValidator

from tiled.structures.array import ArrayStructure
from tiled.structures.awkward import AwkwardStructure
from tiled.structures.sparse import SparseStructure
from tiled.structures.table import TableStructure

if sys.version_info < (3, 10):
    EllipsisType = type(Ellipsis)
else:
    from types import EllipsisType

from typing import (
    Annotated,
    Any,
    Callable,
    Coroutine,
    Dict,
    List,
    Set,
    TypedDict,
    Union,
)

from .utils import import_object

JSON = Dict[str, Union[str, int, float, bool, Dict[str, "JSON"], List["JSON"]]]

Scopes = Set[str]
Query = Any  # for now...
Filters = List[Query]

AnyStructure = Union[TableStructure, ArrayStructure, SparseStructure, AwkwardStructure]

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
    "AnyStructure",
    "AppTask",
    "EllipsisType",
    "JSON",
    "Scopes",
    "Query",
    "Filters",
    "TaskMap",
]
