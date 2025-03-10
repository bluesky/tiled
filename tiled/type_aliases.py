import sys

from tiled.structures.array import ArrayStructure
from tiled.structures.awkward import AwkwardStructure
from tiled.structures.sparse import SparseStructure
from tiled.structures.table import TableStructure

if sys.version_info < (3, 10):
    EllipsisType = type(Ellipsis)
else:
    from types import EllipsisType

from typing import Any, Dict, List, Set, Tuple, Union

JSON = Dict[str, Union[str, int, float, bool, Dict[str, "JSON"], List["JSON"]]]
NDSlice = Union[
    int, slice, Tuple[Union[int, slice, EllipsisType], ...], EllipsisType
]  # TODO Replace this with our Union for a slice/tuple/.../etc.

Scopes = Set[str]
Query = Any  # for now...
Filters = List[Query]

AnyStructure = Union[TableStructure, ArrayStructure, SparseStructure, AwkwardStructure]
