import enum
from typing import Any, Dict, Generic, List, Optional, Tuple, TypeVar

import pydantic
import pydantic.dataclasses
import pydantic.generics

DataT = TypeVar("DataT")


class Error(pydantic.BaseModel):
    code: int
    message: str


class Response(pydantic.generics.GenericModel, Generic[DataT]):
    data: Optional[DataT]
    error: Optional[Error]
    meta: Optional[dict]
    links: Optional[dict]

    @pydantic.validator("error", always=True)
    def check_consistency(cls, v, values):
        if v is not None and values["data"] is not None:
            raise ValueError("must not provide both data and error")
        if v is None and values.get("data") is None:
            raise ValueError("must provide data or error")
        return v


class EntryFields(str, enum.Enum):
    metadata = "metadata"
    structure_family = "structure_family"
    microstructure = "structure.micro"
    macrostructure = "structure.macro"
    count = "count"
    sorting = "sorting"
    specs = "specs"
    none = ""


class StructureFamilies(str, enum.Enum):
    node = "node"
    array = "array"
    dataframe = "dataframe"
    xarray_data_array = "xarray_data_array"
    xarray_dataset = "xarray_dataset"


class NodeAttributes(pydantic.BaseModel):
    structure_family: Optional[StructureFamilies]
    specs: Optional[List[str]]
    metadata: Optional[dict]  # free-form, user-specified dict
    structure: Optional[Any]  # TODO Figure out how to deal with dataclasses in FastAPI
    count: Optional[int]
    sorting: Optional[List[Tuple[str, int]]]


class Resource(pydantic.BaseModel):
    "A JSON API Resource"
    id: str
    meta: Optional[dict]
    links: Optional[dict]
    attributes: NodeAttributes


class AccessAndRefreshTokens(pydantic.BaseModel):
    access_token: str
    expires_in: int
    refresh_token: str
    refresh_token_expires_in: int
    token_type: str


class RefreshToken(pydantic.BaseModel):
    refresh_token: str


class TokenData(pydantic.BaseModel):
    username: Optional[str] = None


class About(pydantic.BaseModel):
    api_version: int
    library_version: str
    formats: Dict[str, List[str]]
    aliases: Dict[str, Dict[str, List[str]]]
    queries: List[str]
    authentication: dict
    meta: dict
