from __future__ import annotations

import dataclasses
import enum
from typing import List, Optional, Union

from .array import ArrayStructure
from .core import StructureFamily
from .dataframe import DataFrameStructure


class SortingDirection(int, enum.Enum):
    ASCENDING = 1
    DECENDING = -1


@dataclasses.dataclass
class SortingItem:
    key: str
    direction: SortingDirection


@dataclasses.dataclass
class NodeStructure:
    contents: Optional[List[NodeAttributes]]
    count: int


@dataclasses.dataclass
class NodeAttributes:
    ancestors: List[str]
    structure_family: Optional[StructureFamily]
    specs: Optional[List[str]]
    metadata: Optional[dict]  # free-form, user-specified dict
    structure: Optional[Union[ArrayStructure, DataFrameStructure, NodeStructure]]
    count: Optional[int]
    sorting: Optional[List[SortingItem]]
