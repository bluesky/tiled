from __future__ import annotations

import enum
import json
import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, Generic, List, Optional, TypeVar, Union

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    ValidationInfo,
    field_validator,
)
from pydantic_core import PydanticCustomError
from typing_extensions import Annotated, TypedDict

from tiled.structures.root import Structure

from ..structures.array import ArrayStructure
from ..structures.awkward import AwkwardStructure
from ..structures.core import STRUCTURE_TYPES, StructureFamily
from ..structures.data_source import Management
from ..structures.sparse import SparseStructure
from ..structures.table import TableStructure

if TYPE_CHECKING:
    import tiled.authn_database.orm
    import tiled.catalog.orm

DataT = TypeVar("DataT")
LinksT = TypeVar("LinksT")
MetaT = TypeVar("MetaT")
StructureT = TypeVar("StructureT", bound=Structure)


MAX_ALLOWED_SPECS = 20


class Error(BaseModel):
    code: int
    message: str


class Response(BaseModel, Generic[DataT, LinksT, MetaT]):
    data: Optional[DataT]
    error: Optional[Error] = None
    links: Optional[LinksT] = None
    meta: Optional[MetaT] = None

    @field_validator("error")
    def check_consistency(cls, v, values):
        if v is not None and values["data"] is not None:
            raise ValueError("must not provide both data and error")
        if v is None and values.get("data") is None:
            raise ValueError("must provide data or error")
        return v


class PaginationLinks(BaseModel):
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
    access_blob = "access_blob"


class NodeStructure(BaseModel):
    contents: Optional[Dict[str, Any]]
    count: int

    model_config = ConfigDict(extra="forbid")


class SortingDirection(int, enum.Enum):
    ASCENDING = 1
    DESCENDING = -1


class SortingItem(BaseModel):
    key: str
    direction: SortingDirection


class Spec(BaseModel, extra="forbid", frozen=True):
    name: Annotated[str, StringConstraints(max_length=255)]
    version: Optional[Annotated[str, StringConstraints(max_length=255)]] = None


# Wait for fix https://github.com/pydantic/pydantic/issues/3957
# Specs = pydantic.conlist(Spec, max_length=20, unique_items=True)
Specs = Annotated[List[Spec], Field(max_length=MAX_ALLOWED_SPECS)]


class Asset(BaseModel):
    data_uri: str
    is_directory: bool
    parameter: Optional[str] = None
    num: Optional[int] = None
    id: Optional[int] = None

    @classmethod
    def from_orm(cls, orm: tiled.catalog.orm.Asset) -> Asset:
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


class Revision(BaseModel):
    revision_number: int
    metadata: dict
    specs: Specs
    access_blob: dict
    time_updated: datetime

    @classmethod
    def from_orm(cls, orm: tiled.catalog.orm.Revision) -> Revision:
        # Trailing underscore in 'metadata_' avoids collision with
        # SQLAlchemy reserved word 'metadata'.
        return cls(
            revision_number=orm.revision_number,
            metadata=orm.metadata_,
            specs=orm.specs,
            access_blob=orm.access_blob,
            time_updated=orm.time_updated,
        )


class DataSource(BaseModel, Generic[StructureT]):
    id: Optional[int] = None
    structure_family: StructureFamily
    structure: Optional[StructureT]
    mimetype: Optional[str] = None
    parameters: dict = {}
    assets: List[Asset] = []
    management: Management = Management.writable

    model_config = ConfigDict(extra="forbid")

    @classmethod
    def from_orm(cls, orm: tiled.catalog.orm.DataSource) -> DataSource:
        if hasattr(orm.structure, "structure"):
            structure = orm.structure.structure
        else:
            structure = None
        return cls(
            id=orm.id,
            structure_family=orm.structure_family,
            structure=structure,
            mimetype=orm.mimetype,
            parameters=orm.parameters,
            assets=[Asset.from_assoc_orm(assoc) for assoc in orm.asset_associations],
            management=orm.management,
        )

    @field_validator("structure", mode="before")
    @classmethod
    def _coerce_structure_family(
        cls, value: Any, info: ValidationInfo
    ) -> Optional[StructureT]:
        "Convert the structure on each data_source from a dict to the appropriate pydantic model."
        if isinstance(value, str):
            value = json.loads(value)
        if isinstance(value, Structure):
            return value
        if isinstance(value, dict[str, Any]):
            family: Optional[StructureFamily] = info.data.get("structure_family")
            if family in STRUCTURE_TYPES:
                return STRUCTURE_TYPES[family].from_json(value)
        return None


class NodeAttributes(BaseModel):
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
    access_blob: Optional[Dict] = None  # free-form, access_policy-specified dict

    sorting: Optional[List[SortingItem]] = None
    data_sources: Optional[List[DataSource]] = None

    model_config = ConfigDict(extra="forbid")


AttributesT = TypeVar("AttributesT")
ResourceMetaT = TypeVar("ResourceMetaT")
ResourceLinksT = TypeVar("ResourceLinksT")


class SelfLinkOnly(BaseModel):
    self: str


class ContainerLinks(BaseModel):
    self: str
    search: str
    full: str


class ArrayLinks(BaseModel):
    self: str
    full: str
    block: str


class AwkwardLinks(BaseModel):
    self: str
    buffers: str
    full: str


class DataFrameLinks(BaseModel):
    self: str
    full: str
    partition: str


class SparseLinks(BaseModel):
    self: str
    full: str
    block: str


resource_links_type_by_structure_family = {
    StructureFamily.array: ArrayLinks,
    StructureFamily.awkward: AwkwardLinks,
    StructureFamily.composite: ContainerLinks,
    StructureFamily.container: ContainerLinks,
    StructureFamily.sparse: SparseLinks,
    StructureFamily.table: DataFrameLinks,
}


class EmptyDict(BaseModel):
    pass


class ContainerMeta(BaseModel):
    count: int


class Resource(BaseModel, Generic[AttributesT, ResourceLinksT, ResourceMetaT]):
    "A JSON API Resource"
    id: Union[str, uuid.UUID]
    attributes: AttributesT
    links: Optional[ResourceLinksT] = None
    meta: Optional[ResourceMetaT] = None


class AccessAndRefreshTokens(BaseModel):
    access_token: str
    expires_in: int
    refresh_token: str
    refresh_token_expires_in: int
    token_type: str


class RefreshToken(BaseModel):
    refresh_token: str


class DeviceCode(BaseModel):
    device_code: str
    grant_type: str


class PrincipalType(str, enum.Enum):
    user = "user"
    service = "service"


class Identity(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: Annotated[str, StringConstraints(max_length=255)]
    provider: Annotated[str, StringConstraints(max_length=255)]
    latest_login: Optional[datetime] = None

    @classmethod
    def from_orm(cls, orm: tiled.authn_database.orm.Identity) -> Identity:
        return cls(id=orm.id, provider=orm.provider, latest_login=orm.latest_login)


class Role(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    name: str
    scopes: List[str]
    # principals

    @classmethod
    def from_orm(cls, orm: tiled.authn_database.orm.Role) -> Role:
        return cls(name=orm.name, scopes=orm.scopes)


class APIKey(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    first_eight: Annotated[str, StringConstraints(min_length=8, max_length=8)]
    expiration_time: Optional[datetime] = None
    note: Optional[Annotated[str, StringConstraints(max_length=255)]] = None
    scopes: List[str]
    latest_activity: Optional[datetime] = None

    @classmethod
    def from_orm(cls, orm: tiled.authn_database.orm.APIKey) -> APIKey:
        return cls(
            first_eight=orm.first_eight,
            expiration_time=orm.expiration_time,
            note=orm.note,
            scopes=orm.scopes,
            latest_activity=orm.latest_activity,
        )


class APIKeyWithSecret(APIKey):
    secret: str  # hex-encoded bytes

    @classmethod
    def from_orm(
        cls, orm: tiled.authn_database.orm.APIKeyWithSecret, secret: str
    ) -> APIKeyWithSecret:
        return cls(
            first_eight=orm.first_eight,
            expiration_time=orm.expiration_time,
            note=orm.note,
            scopes=orm.scopes,
            latest_activity=orm.latest_activity,
            secret=secret,
        )


class Session(BaseModel):
    """
    This related to refresh tokens, which have a session uuid ("sid") claim.

    When the client attempts to use a refresh token, we first check
    here to ensure that the "session", which is associated with a chain
    of refresh tokens that came from a single authentication, are still valid.
    """

    # The id field (primary key) is intentionally not exposed to the application.
    # It is left as an internal database concern.
    model_config = ConfigDict(from_attributes=True)
    uuid: uuid.UUID
    expiration_time: datetime
    revoked: bool
    state: Optional[Dict[Any, Any]]

    @classmethod
    def from_orm(cls, orm: tiled.authn_database.orm.Session) -> Session:
        return cls(
            uuid=orm.uuid,
            expiration_time=orm.expiration_time,
            revoked=orm.revoked,
            state=orm.state,
        )


class Principal(BaseModel):
    "Represents a User or Service"
    # The id field (primary key) is intentionally not exposed to the application.
    # It is left as an internal database concern.
    model_config = ConfigDict(from_attributes=True)
    uuid: uuid.UUID
    type: PrincipalType
    identities: List[Identity] = []
    roles: List[Role] = []
    api_keys: List[APIKey] = []
    sessions: List[Session] = []
    latest_activity: Optional[datetime] = None

    @classmethod
    def from_orm(
        cls,
        orm: tiled.authn_database.orm.Principal,
        latest_activity: Optional[datetime] = None,
    ) -> Principal:
        return cls(
            uuid=orm.uuid,
            type=orm.type,
            identities=[Identity.from_orm(id_) for id_ in orm.identities],
            roles=[Role.from_orm(id_) for id_ in orm.roles],
            api_keys=[APIKey.from_orm(api_key) for api_key in orm.api_keys],
            sessions=[Session.from_orm(session) for session in orm.sessions],
            latest_activity=latest_activity,
        )


class APIKeyRequestParams(BaseModel):
    # Provide an example for expires_in. Otherwise, OpenAPI suggests lifetime=0.
    # If the user is not reading carefully, they will be frustrated when they
    # try to use the instantly-expiring API key!
    expires_in: Optional[int] = Field(
        ..., json_schema_extra={"example": 600}
    )  # seconds
    scopes: Optional[List[str]] = Field(..., json_schema_extra={"example": ["inherit"]})
    note: Optional[str] = None


class PostMetadataRequest(BaseModel):
    id: Optional[str] = None
    structure_family: StructureFamily
    metadata: Dict = {}
    data_sources: List[DataSource] = []
    specs: Specs = []
    access_blob: Optional[Dict] = {}

    # Wait for fix https://github.com/pydantic/pydantic/issues/3957
    # to do this with `unique_items` parameters to `pydantic.constr`.
    @field_validator("specs")
    def specs_uniqueness_validator(cls, v):
        if v is None:
            return None
        for i, value in enumerate(v, start=1):
            if value in v[i:]:
                raise ValueError
        return v


class PutDataSourceRequest(BaseModel):
    data_source: DataSource


class PostMetadataResponse(BaseModel, Generic[ResourceLinksT]):
    id: str
    links: Union[ArrayLinks, DataFrameLinks, SparseLinks]
    metadata: Dict
    data_sources: List[DataSource]
    access_blob: Dict


class PutMetadataResponse(BaseModel, Generic[ResourceLinksT]):
    id: str
    links: Union[ArrayLinks, DataFrameLinks, SparseLinks]
    # May be None if not altered
    metadata: Optional[Dict] = None
    data_sources: Optional[List[DataSource]] = None
    access_blob: Optional[Dict] = None


class DistinctValueInfo(BaseModel):
    value: Any = None
    count: Optional[int] = None


class GetDistinctResponse(BaseModel):
    metadata: Optional[Dict[str, List[DistinctValueInfo]]] = None
    structure_families: Optional[List[DistinctValueInfo]] = None
    specs: Optional[List[DistinctValueInfo]] = None


class PutMetadataRequest(BaseModel):
    # These fields are optional because None means "no changes; do not update".
    metadata: Optional[Dict] = None
    specs: Optional[Specs] = None
    access_blob: Optional[Dict] = None

    # Wait for fix https://github.com/pydantic/pydantic/issues/3957
    # to do this with `unique_items` parameters to `pydantic.constr`.
    @field_validator("specs")
    def specs_uniqueness_validator(cls, v):
        if v is None:
            return None
        for i, value in enumerate(v, start=1):
            if value in v[i:]:
                raise ValueError
        return v


def JSONPatchType(dtype=Any):
    # we use functional syntax with TypedDict here since "from" is a keyword
    return TypedDict(
        "JSONPatchType",
        {
            "op": str,
            "path": str,
            "from": str,
            "value": dtype,
        },
        total=False,
    )


JSONPatchSpec = JSONPatchType(Spec)
JSONPatchAny = JSONPatchType(Any)


class HyphenizedBaseModel(BaseModel):
    # This model configuration allows aliases like "content-type"
    model_config = ConfigDict(alias_generator=lambda f: f.replace("_", "-"))


class PatchMetadataRequest(HyphenizedBaseModel):
    content_type: str

    # These fields are optional because None means "no changes; do not update".
    # Dict for merge-patch:
    metadata: Optional[Union[List[JSONPatchAny], Dict]] = None

    # Specs for merge-patch. left_to_right mode is used to distinguish between
    # merge-patch List[asdict(Spec)] and json-patch List[Dict]
    specs: Optional[Union[Specs, List[JSONPatchSpec]]] = Field(
        union_mode="left_to_right"
    )

    # These fields are optional because None means "no changes; do not update".
    # Dict for merge-patch:
    # Define an alias to override parent class alias generator
    access_blob: Optional[Union[List[JSONPatchAny], Dict]] = Field(
        alias="access_blob", default=None
    )

    @field_validator("specs")
    def specs_uniqueness_validator(cls, v):
        if v is None:
            return None
        if v and isinstance(v[0], Spec):
            # This is the MERGE_PATCH case
            if len(v) != len(set(v)):
                raise PydanticCustomError("specs", "Items must be unique")
        elif v and isinstance(v[0], dict):
            # This is the JSON_PATCH case
            v_new = [v_["value"] for v_ in v if v_["op"] in ["add", "replace"]]
            # Note: uniqueness should be checked with existing specs included,
            # however since we use replace_metadata to eventually write to db this
            # will be caught and an error raised there.
            if len(v_new) != len(set(v_new)):
                raise PydanticCustomError("specs", "Items must be unique")
        return v


class PatchMetadataResponse(BaseModel, Generic[ResourceLinksT]):
    id: str
    links: Union[ArrayLinks, DataFrameLinks, SparseLinks]
    # May be None if not altered
    metadata: Optional[Dict]
    data_sources: Optional[List[DataSource]]
    access_blob: Optional[Dict]


SearchResponse = Response[
    List[Resource[NodeAttributes, Dict, Dict]], PaginationLinks, Dict
]

NodeStructure.model_rebuild()
