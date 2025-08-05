import sys

from tiled.structures.array import ArrayStructure
from tiled.structures.awkward import AwkwardStructure
from tiled.structures.sparse import SparseStructure
from tiled.structures.table import TableStructure

if sys.version_info < (3, 10):
    EllipsisType = type(Ellipsis)
else:
    from types import EllipsisType

from typing import Any, List, Mapping, Sequence, Set, Union

JSON_ITEM = str | int | float | Mapping[str, "JSON_ITEM"] | Sequence["JSON_ITEM"] | None
JSON = Mapping[str, JSON_ITEM]

Scopes = Set[str]
Query = Any  # for now...
Filters = List[Query]

AnyStructure = Union[TableStructure, ArrayStructure, SparseStructure, AwkwardStructure]

__all__ = [
    "AnyStructure",
    "EllipsisType",
    "JSON",
    "Scopes",
    "Query",
    "Filters",
]
