import enum
import uuid
from datetime import datetime
from typing import Dict, Generic, List, Optional, Tuple, TypeVar, Union

import pydantic
import pydantic.dataclasses
import pydantic.generics

DataT = TypeVar("DataT")
LinksT = TypeVar("LinksT")
MetaT = TypeVar("MetaT")


class Error(pydantic.BaseModel):
    code: int
    message: str


class Response(pydantic.generics.GenericModel, Generic[DataT, LinksT, MetaT]):
    data: Optional[DataT]
    error: Optional[Error]
    links: Optional[LinksT]
    meta: Optional[MetaT]

    @pydantic.validator("error", always=True)
    def check_consistency(cls, v, values):
        if v is not None and values["data"] is not None:
            raise ValueError("must not provide both data and error")
        if v is None and values.get("data") is None:
            raise ValueError("must provide data or error")
        return v


class PaginationLinks(pydantic.BaseModel):
    self: str
    next: str
    prev: str
    first: str
    last: str


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


class Structure(pydantic.BaseModel):
    micro: Optional[dict]
    macro: Optional[dict]


class NodeAttributes(pydantic.BaseModel):
    structure_family: Optional[StructureFamilies]
    specs: Optional[List[str]]
    metadata: Optional[dict]  # free-form, user-specified dict
    structure: Optional[Structure]
    count: Optional[int]
    sorting: Optional[List[Tuple]]
    # This seems to hit a bug or limitation in OpenAPI.
    # sorting: Optional[List[Tuple[str, int]]]


AttributesT = TypeVar("AttributesT")
ResourceMetaT = TypeVar("ResourceMetaT")
ResourceLinksT = TypeVar("ResourceLinksT")


class SelfLinkOnly(pydantic.BaseModel):
    self: str


class NodeLinks(pydantic.BaseModel):
    self: str
    search: str
    full: str


class ArrayLinks(pydantic.BaseModel):
    self: str
    full: str
    block: str


class DataFrameLinks(pydantic.BaseModel):
    self: str
    full: str
    partition: str


class XarrayDataArrayLinks(pydantic.BaseModel):
    self: str
    full_variable: str


class XarrayDatasetLinks(pydantic.BaseModel):
    self: str
    full_variable: str
    full_coord: str
    full_dataset: str


resource_links_type_by_structure_family = {
    "node": NodeLinks,
    "array": ArrayLinks,
    "dataframe": DataFrameLinks,
    "xarray_data_array": XarrayDataArrayLinks,
    "xarray_dataset": XarrayDatasetLinks,
}


class EmptyDict(pydantic.BaseModel):
    pass


class NodeMeta(pydantic.BaseModel):
    count: int


class Resource(
    pydantic.generics.GenericModel, Generic[AttributesT, ResourceLinksT, ResourceMetaT]
):
    "A JSON API Resource"
    id: Union[str, uuid.UUID]
    attributes: AttributesT
    links: Optional[ResourceLinksT]
    meta: Optional[ResourceMetaT]


class AccessAndRefreshTokens(pydantic.BaseModel):
    access_token: str
    expires_in: int
    refresh_token: str
    refresh_token_expires_in: int
    token_type: str


class RefreshToken(pydantic.BaseModel):
    refresh_token: str


class AuthenticationMode(str, enum.Enum):
    password = "password"
    external = "external"


class AboutAuthenticationProvider(pydantic.BaseModel):
    provider: str
    mode: AuthenticationMode
    links: Dict[str, str]
    confirmation_message: Optional[str]


class AboutAuthenticationLinks(pydantic.BaseModel):
    whoami: str
    apikey: str
    revoke_apikey: str
    refresh_session: str
    revoke_session: str
    logout: str


class AboutAuthentication(pydantic.BaseModel):
    required: bool
    providers: List[AboutAuthenticationProvider]
    links: Optional[AboutAuthenticationLinks]


class About(pydantic.BaseModel):
    api_version: int
    library_version: str
    formats: Dict[str, List[str]]
    aliases: Dict[str, Dict[str, List[str]]]
    queries: List[str]
    authentication: AboutAuthentication
    links: Dict[str, str]
    meta: dict


class PrincipalType(str, enum.Enum):
    user = "user"
    service = "service"  # TODO Add support for services.


class Identity(pydantic.BaseModel, orm_mode=True):
    id: pydantic.constr(max_length=255)
    provider: pydantic.constr(max_length=255)


class Role(pydantic.BaseModel, orm_mode=True):
    name: str
    scopes: List[str]
    # principals


class APIKeyAttributes(pydantic.BaseModel):
    principal: uuid.UUID
    expiration_time: Optional[datetime]
    note: Optional[pydantic.constr(max_length=255)]
    scopes: List[str]
    latest_activity: Optional[datetime] = None


class APIKey(pydantic.BaseModel, orm_mode=True):
    uuid: uuid.UUID
    expiration_time: Optional[datetime]
    note: Optional[pydantic.constr(max_length=255)]
    scopes: List[str]
    latest_activity: Optional[datetime] = None


class APIKeyWithSecretAttributes(APIKeyAttributes):
    secret: str  # hex-encoded bytes


class Session(pydantic.BaseModel, orm_mode=True):
    """
    This related to refresh tokens, which have a session uuid ("sid") claim.

    When the client attempts to use a refresh token, we first check
    here to ensure that the "session", which is associated with a chain
    of refresh tokens that came from a single authentication, are still valid.
    """

    # The id field (primary key) is intentionally not exposed to the application.
    # It is left as an internal database concern.
    uuid: uuid.UUID
    expiration_time: datetime
    revoked: bool


class PrincipalAttributes(pydantic.BaseModel, orm_mode=True):
    "Represents a User or Service"
    # The id field (primary key) is intentionally not exposed to the application.
    # It is left as an internal database concern.
    type: PrincipalType
    identities: List[Identity] = []
    roles: List[Role] = []
    api_keys: List[APIKey] = []
    sessions: List[Session] = []


class Principal(PrincipalAttributes):
    uuid: uuid.UUID


class APIKeyParams(pydantic.BaseModel):
    lifetime: Optional[int]  # seconds
    scopes: Optional[List[str]]
    note: Optional[str]
