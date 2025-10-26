from datetime import datetime
from typing import Generic, Literal, Optional, TypeVar, Union

from pydantic import BaseModel, Field

from .structures.array import ArrayStructure, BuiltinDtype, StructDtype
from .structures.core import Spec, StructureFamily
from .structures.data_source import Management


class Asset(BaseModel):
    id: Optional[int]  # TODO This should be required, needs work on the SQL
    data_uri: str
    is_directory: bool
    parameter: str
    num: int


StructureT = TypeVar("StructureT")


class DataSource(BaseModel, Generic[StructureT]):
    id: Optional[int]  # TODO This should be required, needs work on the SQL
    structure_family: StructureFamily
    structure: StructureT
    mimetype: str
    parameters: dict
    assets: list[Asset]
    management: Management


class Schema(BaseModel):
    version: int

    def content(self):
        return self.model_dump(exclude={"type", "version"})


class ArraySchema(Schema):
    type: Literal["array-schema"]
    data_type: Union[BuiltinDtype, StructDtype]


class ContainerSchema(Schema):
    type: Literal["container-schema"]
    pass


class Update(BaseModel):
    sequence: int = Field(gt=0)
    timestamp: datetime


class ChildCreated(Update):
    type: Literal["container-child-created"] = "child-created"
    key: str
    structure_family: StructureFamily
    specs: list[Spec]
    metadata: dict
    data_sources: list[DataSource]
    access_blob: dict


class ChildMetadataUpdated(Update):
    type: Literal["container-child-metadata-updated"] = "child-metadata-updated"
    key: str
    specs: list[Spec]
    metadata: dict


class ArrayData(Update):
    type: Literal["array-data"] = "array-data"
    mimetype: str
    shape: tuple[int]
    offset: Optional[tuple[int]]
    block: Optional[tuple[int]]
    payload: bytes
    data_type: Union[BuiltinDtype, StructDtype]


class ArrayPatch(BaseModel):
    offset: tuple[int, ...]
    shape: tuple[int, ...]
    extend: bool


class ArrayRef(Update):
    type: Literal["array-ref"] = "array-ref"
    data_source: DataSource[ArrayStructure]
    patch: Optional[ArrayPatch]
    uri: Optional[str]
    data_type: Union[BuiltinDtype, StructDtype]
