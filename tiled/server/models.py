import enum

import pydantic
import pydantic.dataclasses
import pydantic.generics
from typing import Any, Dict, Generic, List, Optional, TypeVar

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
    reader = "reader"


class EntryFields(str, enum.Enum):
    metadata = "metadata"
    structure_family = "structure_family"
    microstructure = "structure.micro"
    macrostructure = "structure.macro"
    count = "count"
    client_type_hint = "client_type_hint"
    none = ""


class CatalogAttributes(pydantic.BaseModel):
    metadata: Optional[dict]  # free-form, user-specified dict
    count: Optional[int]
    client_type_hint: Optional[str]


class StructureFamilies(str, enum.Enum):
    array = "array"
    dataframe = "dataframe"
    variable = "variable"
    data_array = "data_array"
    dataset = "dataset"


class ReaderAttributes(pydantic.BaseModel):
    metadata: Optional[dict]  # free-form, user-specified dict
    structure_family: Optional[StructureFamilies]
    structure: Optional[Any]  # TODO Figure out how to deal with dataclasses in FastAPI


class Resource(pydantic.BaseModel):
    "A JSON API Resource"
    id: str
    type: EntryType
    meta: Optional[dict]


class CatalogResource(Resource):
    "Representation of a Catalog as a JSON API Resource"
    attributes: CatalogAttributes


class ReaderResource(Resource):
    "Representation of a Reader as a JSON API Resource"
    attributes: ReaderAttributes


class Token(pydantic.BaseModel):
    access_token: str
    token_type: str


class TokenData(pydantic.BaseModel):
    username: Optional[str] = None


class About(pydantic.BaseModel):
    api_version: int
    library_version: str
    formats: Dict[str, List[str]]
    aliases: Dict[str, Dict[str, List[str]]]
    queries: List[str]
