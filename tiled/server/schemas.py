from __future__ import annotations

import enum
import uuid
from datetime import datetime
from typing import Any, Dict, Generic, List, Optional, TypeVar, Union

import pydantic
import pydantic.dataclasses
import pydantic.generics

from ..structures.core import StructureFamily
from .pydantic_array import ArrayStructure
from .pydantic_dataframe import DataFrameStructure
from .pydantic_sparse import SparseStructure

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
    structure = "structure"
    microstructure = "structure.micro"
    macrostructure = "structure.macro"
    count = "count"
    sorting = "sorting"
    specs = "specs"
    references = "references"
    none = ""


class NodeStructure(pydantic.BaseModel):
    contents: Optional[Dict[str, Resource[NodeAttributes, ResourceLinksT, EmptyDict]]]
    count: int


class SortingDirection(int, enum.Enum):
    ASCENDING = 1
    DECENDING = -1


class SortingItem(pydantic.BaseModel):
    key: str
    direction: SortingDirection


class ReferenceDocument(pydantic.BaseModel, extra=pydantic.Extra.forbid):
    label: str
    url: pydantic.AnyUrl

    @classmethod
    def from_json(cls, json_doc):
        return cls(label=json_doc["label"], url=json_doc["url"])


class NodeAttributes(pydantic.BaseModel):
    ancestors: List[str]
    structure_family: Optional[StructureFamily]
    specs: Optional[List[str]]
    metadata: Optional[Dict]  # free-form, user-specified dict
    structure: Optional[
        Union[ArrayStructure, DataFrameStructure, NodeStructure, SparseStructure]
    ]
    sorting: Optional[List[SortingItem]]
    references: Optional[List[ReferenceDocument]]


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


class SparseLinks(pydantic.BaseModel):
    self: str
    full: str
    block: str


resource_links_type_by_structure_family = {
    "node": NodeLinks,
    "array": ArrayLinks,
    "dataframe": DataFrameLinks,
    "sparse": SparseLinks,
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
    meta: Dict


class PrincipalType(str, enum.Enum):
    user = "user"
    service = "service"  # TODO Add support for services.


class Identity(pydantic.BaseModel, orm_mode=True):
    id: pydantic.constr(max_length=255)
    provider: pydantic.constr(max_length=255)
    latest_login: Optional[datetime]


class Role(pydantic.BaseModel, orm_mode=True):
    name: str
    scopes: List[str]
    # principals


class APIKey(pydantic.BaseModel, orm_mode=True):
    first_eight: pydantic.constr(min_length=8, max_length=8)
    expiration_time: Optional[datetime]
    note: Optional[pydantic.constr(max_length=255)]
    scopes: List[str]
    latest_activity: Optional[datetime] = None


class APIKeyWithSecret(APIKey):
    secret: str  # hex-encoded bytes

    @classmethod
    def from_orm(cls, orm, secret):
        return cls(
            first_eight=orm.first_eight,
            expiration_time=orm.expiration_time,
            note=orm.note,
            scopes=orm.scopes,
            latest_activity=orm.latest_activity,
            secret=secret,
        )


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


class Principal(pydantic.BaseModel, orm_mode=True):
    "Represents a User or Service"
    # The id field (primary key) is intentionally not exposed to the application.
    # It is left as an internal database concern.
    uuid: uuid.UUID
    type: PrincipalType
    identities: List[Identity] = []
    roles: List[Role] = []
    api_keys: List[APIKey] = []
    sessions: List[Session] = []
    latest_activity: Optional[datetime] = None

    @classmethod
    def from_orm(cls, orm, latest_activity=None):
        instance = super().from_orm(orm)
        instance.latest_activity = latest_activity
        return instance


class APIKeyRequestParams(pydantic.BaseModel):
    # Provide an example for expires_in. Otherwise, OpenAPI suggests lifetime=0.
    # If the user is not reading carefully, they will be frustrated when they
    # try to use the instantly-expiring API key!
    expires_in: Optional[int] = pydantic.Field(..., example=600)  # seconds
    scopes: Optional[List[str]] = pydantic.Field(..., example=["inherit"])
    note: Optional[str]


class PostMetadataRequest(pydantic.BaseModel):
    structure_family: StructureFamily
    structure: Union[ArrayStructure, DataFrameStructure, SparseStructure]
    metadata: Dict
    specs: List[str]
    references: List[ReferenceDocument]


class PostMetadataResponse(pydantic.BaseModel, Generic[ResourceLinksT]):
    id: str
    metadata: Optional[Dict]  # may be None if validators did not alter metadata
    links: Union[ArrayLinks, DataFrameLinks, SparseLinks]


class DistinctValueInfo(pydantic.BaseModel):
    value: Any
    count: Optional[int]


class GetDistinctResponse(pydantic.BaseModel):
    metadata: Optional[Dict[str, List[DistinctValueInfo]]]
    structure_families: Optional[List[DistinctValueInfo]]
    specs: Optional[List[DistinctValueInfo]]


class PutMetadataRequest(pydantic.BaseModel):
    metadata: Optional[Dict]
    specs: Optional[List[str]]
    references: Optional[List[ReferenceDocument]]


NodeStructure.update_forward_refs()
