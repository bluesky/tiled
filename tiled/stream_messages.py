from datetime import datetime
from typing import Generic, Literal, Optional, TypeVar

from pydantic import BaseModel, Field

from .structures.array import ArrayStructure
from .structures.core import Spec, StructureFamily
from .structures.data_source import Management


class Asset(BaseModel):
    id: int
    data_uri: str
    is_directory: bool
    parameter: str
    num: int


StructureT = TypeVar("StructureT")


class DataSource(BaseModel, Generic[StructureT]):
    id: int
    structure_family: StructureFamily
    structure: StructureT
    mimetype: str
    parameters: dict
    assets: list[Asset]
    management: Management


class Update(BaseModel):
    sequence: int = Field(gt=0)
    timestamp: datetime


class ChildCreated(Update):
    type: Literal["child-created"] = "child-created"
    key: str
    structure_family: StructureFamily
    specs: list[Spec]
    metadata: dict
    data_sources: list[DataSource]


class ChildMetadataUpdated(Update):
    type: Literal["child-metadata-updated"] = "child-metadata-updated"
    key: str
    specs: list[Spec]
    metadata: dict


class ArrayDataUpdated(Update):
    type: Literal["array-data"] = "array-data"
    mimetype: str
    shape: tuple[int]
    offset: Optional[tuple[int]]
    block: Optional[tuple[int]]
    payload: bytes


class ArrayPatch(BaseModel):
    offset: tuple[int, ...]
    shape: tuple[int, ...]
    extend: bool


class ArrayRefUpdated(Update):
    type: Literal["array-ref"] = "array-ref"
    data_source: DataSource[ArrayStructure]
    patch: Optional[ArrayPatch]


MESSAGE_TYPES = {
    "child-created": ChildCreated,
    "child-metadata-updated": ChildMetadataUpdated,
    "array-data": ArrayDataUpdated,
    "array-ref": ArrayRefUpdated,
}
