from __future__ import annotations

import enum
import uuid
from datetime import datetime
from typing import Any, Dict, Generic, List, Optional, TypeVar, Union

import pydantic.generics
from pydantic import Field, StringConstraints
from typing_extensions import Annotated, TypeAliasType

from ..structures.core import StructureFamily
from ..structures.data_source import Management
from .pydantic_array import ArrayStructure
from .pydantic_awkward import AwkwardStructure
from .pydantic_sparse import SparseStructure
from .pydantic_table import TableStructure

DataT = TypeVar("DataT")
LinksT = TypeVar("LinksT")
MetaT = TypeVar("MetaT")


class Error(pydantic.BaseModel):
    code: int
    message: str


class Response(pydantic.generics.GenericModel, Generic[DataT, LinksT, MetaT]):
    data: Optional[DataT]
    error: Optional[Error] = None
    links: Optional[LinksT] = None
    meta: Optional[MetaT] = None

    @pydantic.field_validator("error")
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
    count = "count"
    sorting = "sorting"
    specs = "specs"
    data_sources = "data_sources"
    none = ""


class NodeStructure(pydantic.BaseModel):
    # contents: Optional[Dict[str, Resource[NodeAttributes, ResourceLinksT, EmptyDict]]]
    # contents: Optional[Dict[str, Resource[NodeAttributes, Union[ArrayLinks, AwkwardLinks, ContainerLinks,
    # SparseLinks, DataFrameLinks], EmptyDict]]]
    # contents: Optional[Union[Dict[str, Resource[NodeAttributes, ResourceLinksT, EmptyDict]]]]
    contents: Optional[Dict[str, Any]]

    count: int

    class Config:
        smart_union = True


class SortingDirection(int, enum.Enum):
    ASCENDING = 1
    DECENDING = -1


class SortingItem(pydantic.BaseModel):
    key: str
    direction: SortingDirection


class Spec(pydantic.BaseModel, extra="forbid", frozen=True):
    name: Annotated[str, StringConstraints(max_length=255)]
    version: Optional[Annotated[str, StringConstraints(max_length=255)]] = None


# Wait for fix https://github.com/pydantic/pydantic/issues/3957
# Specs = pydantic.conlist(Spec, max_length=20, unique_items=True)
Specs = Annotated[List[Spec], Field(max_length=20)]


class Asset(pydantic.BaseModel):
    data_uri: str
    is_directory: bool
    parameter: Optional[str] = None
    num: Optional[int] = None
    id: Optional[int] = None

    @classmethod
    def from_orm(cls, orm):
        return cls(
            data_uri=orm.data_uri,
            is_directory=orm.is_directory,
            id=orm.id,
        )

    @classmethod
    def from_assoc_orm(cls, orm):
        return cls(
            data_uri=orm.asset.data_uri,
            is_directory=orm.asset.is_directory,
            parameter=orm.parameter,
            num=orm.num,
            id=orm.asset.id,
        )


class Revision(pydantic.BaseModel):
    revision_number: int
    metadata: dict
    specs: Specs
    time_updated: datetime

    @classmethod
    def from_orm(cls, orm):
        # Trailing underscore in 'metadata_' avoids collision with
        # SQLAlchemy reserved word 'metadata'.
        return cls(
            revision_number=orm.revision_number,
            metadata=orm.metadata_,
            specs=orm.specs,
            time_updated=orm.time_updated,
        )


class DataSource(pydantic.BaseModel):
    id: Optional[int] = None
    structure_family: Optional[StructureFamily] = None
    structure: Optional[
        Union[
            ArrayStructure,
            AwkwardStructure,
            SparseStructure,
            NodeStructure,
            TableStructure,
        ]
    ] = None
    mimetype: Optional[str] = None
    parameters: dict = {}
    assets: List[Asset] = []
    management: Management = Management.writable

    class Config:
        extra = "forbid"

    @classmethod
    def from_orm(cls, orm):
        return cls(
            id=orm.id,
            structure_family=orm.structure_family,
            structure=getattr(orm.structure, "structure", None),
            mimetype=orm.mimetype,
            parameters=orm.parameters,
            assets=[Asset.from_assoc_orm(assoc) for assoc in orm.asset_associations],
            management=orm.management,
        )


class NodeAttributes(pydantic.BaseModel):
    ancestors: List[str]
    structure_family: Optional[StructureFamily] = None
    specs: Optional[Specs] = None
    metadata: Optional[Dict] = None  # free-form, user-specified dict
    structure: Optional[
        Union[
            ArrayStructure,
            AwkwardStructure,
            SparseStructure,
            NodeStructure,
            TableStructure,
        ]
    ] = None

    sorting: Optional[List[SortingItem]] = None
    data_sources: Optional[List[DataSource]] = None

    class Config:
        extra = "forbid"


AttributesT = TypeVar("AttributesT")
ResourceMetaT = TypeVar("ResourceMetaT")
ResourceLinksT = TypeVar("ResourceLinksT")


class SelfLinkOnly(pydantic.BaseModel):
    self: str


class ContainerLinks(pydantic.BaseModel):
    self: str
    search: str
    full: str


class ArrayLinks(pydantic.BaseModel):
    self: str
    full: str
    block: str


class AwkwardLinks(pydantic.BaseModel):
    self: str
    buffers: str
    full: str


class DataFrameLinks(pydantic.BaseModel):
    self: str
    full: str
    partition: str


class SparseLinks(pydantic.BaseModel):
    self: str
    full: str
    block: str


resource_links_type_by_structure_family = {
    StructureFamily.array: ArrayLinks,
    StructureFamily.awkward: AwkwardLinks,
    StructureFamily.container: ContainerLinks,
    StructureFamily.sparse: SparseLinks,
    StructureFamily.table: DataFrameLinks,
}


class EmptyDict(pydantic.BaseModel):
    pass


class ContainerMeta(pydantic.BaseModel):
    count: int


class Resource(
    pydantic.generics.GenericModel, Generic[AttributesT, ResourceLinksT, ResourceMetaT]
):
    "A JSON API Resource"
    id: Union[str, uuid.UUID]
    attributes: AttributesT
    links: Optional[ResourceLinksT] = None
    meta: Optional[ResourceMetaT] = None


class AccessAndRefreshTokens(pydantic.BaseModel):
    access_token: str
    expires_in: int
    refresh_token: str
    refresh_token_expires_in: int
    token_type: str


class RefreshToken(pydantic.BaseModel):
    refresh_token: str


class DeviceCode(pydantic.BaseModel):
    device_code: str
    grant_type: str


class AuthenticationMode(str, enum.Enum):
    password = "password"
    external = "external"


class AboutAuthenticationProvider(pydantic.BaseModel):
    provider: str
    mode: AuthenticationMode
    links: Dict[str, str]
    confirmation_message: Optional[str] = None


class AboutAuthenticationLinks(pydantic.BaseModel):
    whoami: str
    apikey: str
    refresh_session: str
    revoke_session: str
    logout: str


class AboutAuthentication(pydantic.BaseModel):
    required: bool
    providers: List[AboutAuthenticationProvider]
    links: Optional[AboutAuthenticationLinks] = None


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
    service = "service"


class Identity(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(from_attributes=True)
    id: Annotated[str, StringConstraints(max_length=255)]
    provider: Annotated[str, StringConstraints(max_length=255)]
    latest_login: Optional[datetime] = None


class Role(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(from_attributes=True)
    name: str
    scopes: List[str]
    # principals


class APIKey(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(from_attributes=True)
    first_eight: Annotated[str, StringConstraints(min_length=8, max_length=8)]
    expiration_time: Optional[datetime] = None
    note: Optional[Annotated[str, StringConstraints(max_length=255)]] = None
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


class Session(pydantic.BaseModel):
    """
    This related to refresh tokens, which have a session uuid ("sid") claim.

    When the client attempts to use a refresh token, we first check
    here to ensure that the "session", which is associated with a chain
    of refresh tokens that came from a single authentication, are still valid.
    """

    # The id field (primary key) is intentionally not exposed to the application.
    # It is left as an internal database concern.
    model_config = pydantic.ConfigDict(from_attributes=True)
    uuid: uuid.UUID
    expiration_time: datetime
    revoked: bool


class Principal(pydantic.BaseModel):
    "Represents a User or Service"
    # The id field (primary key) is intentionally not exposed to the application.
    # It is left as an internal database concern.
    model_config = pydantic.ConfigDict(from_attributes=True)
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
    note: Optional[str] = None


class PostMetadataRequest(pydantic.BaseModel):
    id: Optional[str] = None
    structure_family: StructureFamily
    metadata: Dict = {}
    data_sources: List[DataSource] = []
    specs: Specs = []

    # Wait for fix https://github.com/pydantic/pydantic/issues/3957
    # to do this with `unique_items` parameters to `pydantic.constr`.
    @pydantic.field_validator("specs")
    def specs_uniqueness_validator(cls, v):
        if v is None:
            return None
        for i, value in enumerate(v, start=1):
            if value in v[i:]:
                raise ValueError
        return v


class PutDataSourceRequest(pydantic.BaseModel):
    data_source: DataSource


class PostMetadataResponse(pydantic.BaseModel, Generic[ResourceLinksT]):
    id: str
    links: Union[ArrayLinks, DataFrameLinks, SparseLinks]
    metadata: Dict
    data_sources: List[DataSource]


class PutMetadataResponse(pydantic.BaseModel, Generic[ResourceLinksT]):
    id: str
    links: Union[ArrayLinks, DataFrameLinks, SparseLinks]
    # May be None if not altered
    metadata: Optional[Dict] = None
    data_sources: Optional[List[DataSource]] = None


class DistinctValueInfo(pydantic.BaseModel):
    value: Any = None
    count: Optional[int] = None


class GetDistinctResponse(pydantic.BaseModel):
    metadata: Optional[Dict[str, List[DistinctValueInfo]]] = None
    structure_families: Optional[List[DistinctValueInfo]] = None
    specs: Optional[List[DistinctValueInfo]] = None


class PutMetadataRequest(pydantic.BaseModel):
    # These fields are optional because None means "no changes; do not update".
    metadata: Optional[Dict] = None
    specs: Optional[Specs] = None

    # Wait for fix https://github.com/pydantic/pydantic/issues/3957
    # to do this with `unique_items` parameters to `pydantic.constr`.
    @pydantic.field_validator("specs")
    def specs_uniqueness_validator(cls, v):
        if v is None:
            return None
        for i, value in enumerate(v, start=1):
            if value in v[i:]:
                raise ValueError
        return v


NodeStructure.update_forward_refs()
PositiveIntList = TypeAliasType(
    "PositiveIntList",
    Union[ArrayLinks, AwkwardLinks, ContainerLinks, SparseLinks, DataFrameLinks],
)
