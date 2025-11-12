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


class TableSchema(Schema):
    type: Literal["table-schema"]
    arrow_schema: str


class Update(BaseModel):
    sequence: int = Field(gt=0)
    timestamp: datetime


class ChildCreated(Update):
    type: Literal["container-child-created"] = "container-child-created"
    key: str
    structure_family: StructureFamily
    specs: list[Spec]
    metadata: dict
    data_sources: list[DataSource]
    access_blob: dict


class ChildMetadataUpdated(Update):
    type: Literal[
        "container-child-metadata-updated"
    ] = "container-child-metadata-updated"
    key: str
    specs: list[Spec]
    metadata: dict


class ArrayData(Update):
    type: Literal["array-data"] = "array-data"
    mimetype: str
    shape: tuple[int, ...]
    offset: Optional[tuple[int, ...]]
    block: Optional[tuple[int, ...]]
    payload: bytes
    data_type: Union[BuiltinDtype, StructDtype]


class ArrayPatch(BaseModel):
    offset: tuple[int, ...]
    shape: tuple[int, ...]


class ArrayRef(Update):
    type: Literal["array-ref"] = "array-ref"
    data_source: DataSource[ArrayStructure]
    patch: Optional[ArrayPatch]
    uri: Optional[str]
    shape: tuple[int, ...]
    data_type: Union[BuiltinDtype, StructDtype]


class TableData(Update):
    type: Literal["table-data"] = "table-data"
    mimetype: str
    # partition=None means a write to the entire table, an old design choice
    # that may need revisiting.
    partition: Optional[int]
    append: bool
    payload: bytes
    arrow_schema: str
