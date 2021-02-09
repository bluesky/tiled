import enum
import pydantic
import pydantic.generics
from typing import Generic, Optional, TypeVar, Tuple, Any


DataT = TypeVar("DataT")


class Error(pydantic.BaseModel):
    code: int
    message: str


class Response(pydantic.generics.GenericModel, Generic[DataT]):
    data: Optional[DataT]
    error: Optional[Error]
    meta: Optional[dict]

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
    description = "description"
    count = "count"


class CatalogAttributes(pydantic.BaseModel):
    metadata: Optional[dict]  # free-form, user-specified dict
    count: Optional[int]


class DataSourceDescription(pydantic.BaseModel):
    dtype: str  # TODO explode into sub-model
    chunks: Any  # Tuple[Tuple]
    shape: Any  # Tuple


class CatalogEntryAttributes(CatalogAttributes):
    key: str


class DataSourceAttributes(pydantic.BaseModel):
    metadata: Optional[dict]  # free-form, user-specified dict
    description: Optional[DataSourceDescription]


class DataSourceEntryAttributes(DataSourceAttributes):
    key: str


class Resource(pydantic.BaseModel):
    "A JSON API Resource"
    id: str
    type: EntryType


class CatalogResource(Resource):
    "Representation of a Catalog as a JSON API Resource"
    id: str
    type: EntryType
    attributes: CatalogAttributes


class DataSourceResource(Resource):
    "Representation of a DataSource as a JSON API Resource"
    id: str
    type: EntryType
    attributes: DataSourceAttributes


BlockIndex = Tuple
