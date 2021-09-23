import enum

import pydantic
import pydantic.dataclasses
import pydantic.generics
from typing import Any, Dict, Generic, List, Optional, Tuple, TypeVar

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
    tree = "tree"
    reader = "reader"


class EntryFields(str, enum.Enum):
    metadata = "metadata"
    structure_family = "structure_family"
    microstructure = "structure.micro"
    macrostructure = "structure.macro"
    count = "count"
    specs = "specs"
    none = ""


class TreeAttributes(pydantic.BaseModel):
    metadata: Optional[dict]  # free-form, user-specified dict
    count: Optional[int]
    sorting: Optional[List[Tuple[str, int]]]
    specs: Optional[List[str]]


class StructureFamilies(str, enum.Enum):
    array = "array"
    dataframe = "dataframe"
    structured_array_tabular = "structured_array_tabular"
    structured_array_generic = "structured_array_generic"
    variable = "variable"
    data_array = "data_array"
    dataset = "dataset"


class ReaderAttributes(pydantic.BaseModel):
    metadata: Optional[dict]  # free-form, user-specified dict
    structure_family: Optional[StructureFamilies]
    structure: Optional[Any]  # TODO Figure out how to deal with dataclasses in FastAPI
    specs: Optional[List[str]]


class Resource(pydantic.BaseModel):
    "A JSON API Resource"
    id: str
    type: EntryType
    meta: Optional[dict]
    links: Optional[dict]


class TreeResource(Resource):
    "Representation of a Tree as a JSON API Resource"
    attributes: TreeAttributes


class ReaderResource(Resource):
    "Representation of a Reader as a JSON API Resource"
    attributes: ReaderAttributes


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
