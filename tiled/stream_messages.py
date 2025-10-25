from datetime import datetime
from typing import Generic, Literal, Optional, TypeVar, Union

from pydantic import BaseModel, Field

from .media_type_registration import default_deserialization_registry
from .structures.array import ArrayStructure, BuiltinDtype, StructDtype
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


class ChildMetadataUpdate(Update):
    type: Literal["container-child-metadata-updated"] = "child-metadata-updated"
    key: str
    specs: list[Spec]
    metadata: dict


class ArrayDataUpdate(Update):
    type: Literal["array-data"] = "array-data"
    mimetype: str
    shape: tuple[int]
    offset: Optional[tuple[int]]
    block: Optional[tuple[int]]
    payload: bytes
    data_type: Union[BuiltinDtype, StructDtype]

    def data(self):
        "Get array"
        # Registration occurs on import. Ensure this is imported.
        from .serialization import array

        del array

        # Decode payload (bytes) into array.
        deserializer = default_deserialization_registry.dispatch("array", self.mimetype)
        return deserializer(self.payload, self.data_type.to_numpy_dtype(), self.shape)


class ArrayPatch(BaseModel):
    offset: tuple[int, ...]
    shape: tuple[int, ...]
    extend: bool


class ArrayRefUpdate(Update):
    type: Literal["array-ref"] = "array-ref"
    data_source: DataSource[ArrayStructure]
    patch: Optional[ArrayPatch]


SCHEMA_MESSAGE_TYPES = {
    "array-schema": ArraySchema,
    "container-schema": ContainerSchema,
}
UPDATE_MESSAGE_TYPES = {
    "container-child-created": ChildCreated,
    "container-child-metadata-updated": ChildMetadataUpdate,
    "array-data": ArrayDataUpdate,
    "array-ref": ArrayRefUpdate,
}
