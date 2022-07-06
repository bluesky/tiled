"""
The ArrayStructure class in tiled.structure.array is implemeneted with Python
built-in dataclasses. This is an implementation of the same structure in pydantic.
"""
from __future__ import annotations

from typing import List, Optional, Union

import pydantic

from ..structures.core import StructureFamily
from ..structures.node import SortingDirection
from .pydantic_array import ArrayStructure
from .pydantic_dataframe import DataFrameStructure


class NodeStructure(pydantic.BaseModel):
    contents: Optional[List[NodeAttributes]]
    count: int


class SortingItem(pydantic.BaseModel):
    key: str
    direction: SortingDirection


class NodeAttributes(pydantic.BaseModel):
    ancestors: List[str]
    structure_family: Optional[StructureFamily]
    specs: Optional[List[str]]
    metadata: Optional[dict]  # free-form, user-specified dict
    structure: Optional[Union[ArrayStructure, DataFrameStructure, NodeStructure]]
    count: Optional[int]
    sorting: Optional[List[SortingItem]]


NodeStructure.update_forward_refs()
