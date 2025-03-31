import sys

from tiled.structures.array import ArrayStructure
from tiled.structures.awkward import AwkwardStructure
from tiled.structures.sparse import SparseStructure
from tiled.structures.table import TableStructure

if sys.version_info < (3, 10):
    EllipsisType = type(Ellipsis)
else:
    from types import EllipsisType

from typing import Any, Dict, List, Set, Union

JSON = Dict[str, Union[str, int, float, bool, Dict[str, "JSON"], List["JSON"]]]

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
