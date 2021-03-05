import enum

import pydantic
import pydantic.dataclasses
import pydantic.generics
from typing import Any, Generic, Optional, TypeVar

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


class EntryType(str, enum.Enum):
    catalog = "catalog"
    datasource = "datasource"


class EntryFields(str, enum.Enum):
    metadata = "metadata"
    container = "container"
    structure = "structure"
    count = "count"
    client_type_hint = "client_type_hint"
    none = ""


class CatalogAttributes(pydantic.BaseModel):
    metadata: Optional[dict]  # free-form, user-specified dict
    count: Optional[int]
    client_type_hint: Optional[str]


class Container(str, enum.Enum):
    array = "array"
    variable = "variable"
    data_array = "data_array"
    dataset = "dataset"


class DataSourceAttributes(pydantic.BaseModel):
    metadata: Optional[dict]  # free-form, user-specified dict
    container: Optional[Container]
    structure: Optional[Any]  # TODO Figure out how to deal with dataclasses in FastAPI


class Resource(pydantic.BaseModel):
    "A JSON API Resource"
    id: str
    type: EntryType
    meta: Optional[dict]


class CatalogResource(Resource):
    "Representation of a Catalog as a JSON API Resource"
    attributes: CatalogAttributes


class DataSourceResource(Resource):
    "Representation of a DataSource as a JSON API Resource"
    attributes: DataSourceAttributes


class Token(pydantic.BaseModel):
    access_token: str
    token_type: str


class TokenData(pydantic.BaseModel):
    username: Optional[str] = None
