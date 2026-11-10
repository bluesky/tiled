from __future__ import annotations

import enum
import uuid
from datetime import UTC, datetime
from http import HTTPStatus
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Generic,
    Literal,
    TypeVar,
)

import pydantic.generics
from pydantic import (
    AnyHttpUrl,
    AwareDatetime,
    ConfigDict,
    Field,
    SecretStr,
    StringConstraints,
    field_validator,
)
from pydantic_core import PydanticCustomError
from typing_extensions import TypedDict

from tiled.structures.ragged import RaggedStructure

from ..structures.array import ArrayStructure
from ..structures.awkward import AwkwardStructure
from ..structures.bytes import BytesStructure
from ..structures.core import STRUCTURE_TYPES, Spec, StructureFamily
from ..structures.data_source import Management
from ..structures.sparse import SparseStructure
from ..structures.table import TableStructure

if TYPE_CHECKING:
    import tiled.authn_database.orm
    import tiled.catalog.orm

DataT = TypeVar("DataT")
LinksT = TypeVar("LinksT")
MetaT = TypeVar("MetaT")
StructureT = TypeVar("StructureT")


MAX_ALLOWED_SPECS = 20


class Error(pydantic.BaseModel):
    code: int
    message: str


class Response(pydantic.BaseModel, Generic[DataT, LinksT, MetaT]):
    data: DataT | None
    error: Error | None = None
    links: LinksT | None = None
    meta: MetaT | None = None

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


class EntryFields(enum.StrEnum):
    metadata = "metadata"
    structure_family = "structure_family"
    structure = "structure"
    count = "count"
    sorting = "sorting"
    specs = "specs"
    data_sources = "data_sources"
    none = ""
    access_tags = "access_tags"


class NodeStructure(pydantic.BaseModel):
    contents: dict[str, Any] | None
    count: int

    model_config = pydantic.ConfigDict(extra="forbid")


class SortingDirection(int, enum.Enum):
    ASCENDING = 1
    DESCENDING = -1


class SortingItem(pydantic.BaseModel):
    key: str
    direction: SortingDirection


# class Spec(pydantic.BaseModel, extra="forbid", frozen=True):
#     name: Annotated[str, StringConstraints(max_length=255)]
#     version: Optional[Annotated[str, StringConstraints(max_length=255)]] = None


# Wait for fix https://github.com/pydantic/pydantic/issues/3957
# Specs = pydantic.conlist(Spec, max_length=20, unique_items=True)
Specs = Annotated[list[Spec], Field(max_length=MAX_ALLOWED_SPECS)]


class Asset(pydantic.BaseModel):
    data_uri: str
    is_directory: bool
    size: int | None = None
    parameter: str | None = None
    num: int | None = None
    id: int | None = None

    @classmethod
    def from_orm(cls, orm: tiled.catalog.orm.Asset) -> Asset:
        return cls(
            data_uri=orm.data_uri,
            is_directory=orm.is_directory,
            size=orm.size,
            id=orm.id,
        )

    @classmethod
    def from_assoc_orm(cls, orm):
        return cls(
            data_uri=orm.asset.data_uri,
            is_directory=orm.asset.is_directory,
            size=orm.asset.size,
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
    def from_orm(cls, orm: tiled.catalog.orm.Revision) -> Revision:
        # Trailing underscore in 'metadata_' avoids collision with
        # SQLAlchemy reserved word 'metadata'.
        return cls(
            revision_number=orm.revision_number,
            metadata=orm.metadata_,
            specs=orm.specs,
            time_updated=orm.time_updated,
        )


class DataSource(pydantic.BaseModel, Generic[StructureT]):
    id: int | None = None
    structure_family: StructureFamily
    structure: StructureT | None
    mimetype: str | None = None
    parameters: dict = {}
    properties: dict = {}
    assets: list[Asset] = []
    management: Management = Management.writable

    model_config = pydantic.ConfigDict(extra="forbid")

    @classmethod
    def from_orm(
        cls, orm: tiled.catalog.orm.DataSource, include_assets: bool = True
    ) -> DataSource:
        if hasattr(orm.structure, "structure"):
            structure_cls = STRUCTURE_TYPES[orm.structure_family]
            structure = structure_cls.from_json(orm.structure.structure)
        else:
            structure = None
        # `asset_associations` is not eagerly loaded (lazy="raise"); only touch it
        # when the caller explicitly needs assets and has loaded them. This keeps
        # the common metadata/structure/truthiness paths from materializing all
        # assets of a large file-sequence data source.
        if include_assets:
            assets = [Asset.from_assoc_orm(assoc) for assoc in orm.asset_associations]
        else:
            assets = []
        return cls(
            id=orm.id,
            structure_family=orm.structure_family,
            structure=structure,
            mimetype=orm.mimetype,
            parameters=orm.parameters,
            properties=orm.properties,
            assets=assets,
            management=orm.management,
        )


class NodeAttributes(pydantic.BaseModel):
    ancestors: list[str]
    structure_family: StructureFamily | None = None
    specs: Specs | None = None
    metadata: dict | None = None  # free-form, user-specified dict
    structure: (
        ArrayStructure
        | AwkwardStructure
        | BytesStructure
        | RaggedStructure
        | SparseStructure
        | NodeStructure
        | TableStructure
        | None
    ) = None
    # list of tags that control access to this node
    access_tags: list[str] | None = None

    sorting: list[SortingItem] | None = None
    data_sources: list[DataSource] | None = None

    model_config = pydantic.ConfigDict(extra="forbid")


AttributesT = TypeVar("AttributesT")
ResourceMetaT = TypeVar("ResourceMetaT")
ResourceLinksT = TypeVar("ResourceLinksT")


class SelfLinkOnly(pydantic.BaseModel):
    self: str


class ContainerLinks(pydantic.BaseModel):
    self: str
    search: str
    search_deep: str | None = None
    full: str


class ArrayLinks(pydantic.BaseModel):
    self: str
    full: str
    block: str


class AwkwardLinks(pydantic.BaseModel):
    self: str
    buffers: str
    full: str


class BytesLinks(pydantic.BaseModel):
    self: str


class DataFrameLinks(pydantic.BaseModel):
    self: str
    full: str
    partition: str


class RaggedLinks(pydantic.BaseModel):
    self: str
    full: str
    block: str


class SparseLinks(pydantic.BaseModel):
    self: str
    full: str
    block: str


resource_links_type_by_structure_family = {
    StructureFamily.array: ArrayLinks,
    StructureFamily.awkward: AwkwardLinks,
    StructureFamily.bytes: BytesLinks,
    StructureFamily.container: ContainerLinks,
    StructureFamily.ragged: RaggedLinks,
    StructureFamily.sparse: SparseLinks,
    StructureFamily.table: DataFrameLinks,
}


class EmptyDict(pydantic.BaseModel):
    pass


class ContainerMeta(pydantic.BaseModel):
    count: int


class Resource(pydantic.BaseModel, Generic[AttributesT, ResourceLinksT, ResourceMetaT]):
    "A JSON API Resource"

    id: str | uuid.UUID
    attributes: AttributesT
    links: ResourceLinksT | None = None
    meta: ResourceMetaT | None = None


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


class PrincipalType(enum.StrEnum):
    user = "user"
    service = "service"


class Identity(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(from_attributes=True)
    id: Annotated[str, StringConstraints(max_length=255)]
    provider: Annotated[str, StringConstraints(max_length=255)]
    latest_login: datetime | None = None

    @classmethod
    def from_orm(cls, orm: tiled.authn_database.orm.Identity) -> Identity:
        return cls(id=orm.id, provider=orm.provider, latest_login=orm.latest_login)


class Role(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(from_attributes=True)
    name: str
    scopes: list[str]
    # principals

    @classmethod
    def from_orm(cls, orm: tiled.authn_database.orm.Role) -> Role:
        return cls(name=orm.name, scopes=orm.scopes)


class APIKey(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(from_attributes=True)
    first_eight: Annotated[str, StringConstraints(min_length=8, max_length=8)]
    expiration_time: datetime | None = None
    note: Annotated[str, StringConstraints(max_length=255)] | None = None
    scopes: list[str]
    access_tags: list[str] | None = None
    latest_activity: datetime | None = None

    @classmethod
    def from_orm(cls, orm: tiled.authn_database.orm.APIKey) -> APIKey:
        return cls(
            first_eight=orm.first_eight,
            expiration_time=orm.expiration_time,
            note=orm.note,
            scopes=orm.scopes,
            access_tags=orm.access_tags,
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
            access_tags=orm.access_tags,
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
    state: dict[Any, Any] | None

    @classmethod
    def from_orm(cls, orm: tiled.authn_database.orm.Session) -> Session:
        return cls(
            uuid=orm.uuid,
            expiration_time=orm.expiration_time,
            revoked=orm.revoked,
            state=orm.state,
        )


class Principal(pydantic.BaseModel):
    "Represents a User or Service"

    # The id field (primary key) is intentionally not exposed to the application.
    # It is left as an internal database concern.
    model_config = pydantic.ConfigDict(from_attributes=True)
    uuid: uuid.UUID
    type: PrincipalType
    identities: list[Identity] = []
    roles: list[Role] = []
    api_keys: list[APIKey] = []
    sessions: list[Session] = []
    latest_activity: datetime | None = None
    access_token: SecretStr | None = Field(exclude=True, default=None)

    @classmethod
    def from_orm(
        cls,
        orm: tiled.authn_database.orm.Principal,
        latest_activity: datetime | None = None,
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


class APIKeyRequestParams(pydantic.BaseModel):
    # Provide an example for expires_in. Otherwise, OpenAPI suggests lifetime=0.
    # If the user is not reading carefully, they will be frustrated when they
    # try to use the instantly-expiring API key!
    expires_in: int | None = pydantic.Field(
        ..., json_schema_extra={"example": 600}
    )  # seconds
    access_tags: list[str] | None = pydantic.Field(
        default=None, json_schema_extra={"example": ["writing_tag", "public"]}
    )
    scopes: list[str] | None = pydantic.Field(
        ..., json_schema_extra={"example": ["inherit"]}
    )
    note: str | None = None


# BACK-COMPAT: remove together with the access_blob helpers in _backcompat.py.
_ACCESS_BLOB_DEPRECATION = (
    "Deprecated in favor of `access_tags`. Accepted from clients older than "
    "v0.2.19 and translated server-side."
)


class PostMetadataRequest(pydantic.BaseModel):
    id: str | None = None
    structure_family: StructureFamily
    metadata: dict = {}
    data_sources: list[DataSource] = []
    specs: Specs = []
    access_tags: list[str] | None = None
    # BACK-COMPAT: remove together with the access_blob helpers in _backcompat.py.
    access_blob: dict | None = pydantic.Field(
        default=None, deprecated=_ACCESS_BLOB_DEPRECATION
    )

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

    @pydantic.model_validator(mode="after")
    def narrow_structure_type(self):
        "Convert the structure on each data_source from a dict to the appropriate pydantic model."
        for data_source in self.data_sources:
            if self.structure_family != StructureFamily.container:
                structure_cls = STRUCTURE_TYPES[self.structure_family]
                if data_source.structure is not None:
                    data_source.structure = structure_cls.from_json(
                        data_source.structure
                    )
        return self


class PutDataSourceRequest(pydantic.BaseModel):
    data_source: DataSource


class PostMetadataResponse(pydantic.BaseModel, Generic[ResourceLinksT]):
    id: str
    links: ArrayLinks | DataFrameLinks | SparseLinks
    metadata: dict
    data_sources: list[DataSource]
    access_tags: list[str]


class PutMetadataResponse(pydantic.BaseModel, Generic[ResourceLinksT]):
    id: str
    links: ArrayLinks | DataFrameLinks | SparseLinks
    # May be None if not altered
    metadata: dict | None = None
    data_sources: list[DataSource] | None = None
    access_tags: list[str] | None = None


class DistinctValueInfo(pydantic.BaseModel):
    value: Any = None
    count: int | None = None


class GetDistinctResponse(pydantic.BaseModel):
    metadata: dict[str, list[DistinctValueInfo]] | None = None
    structure_families: list[DistinctValueInfo] | None = None
    specs: list[DistinctValueInfo] | None = None


class PutMetadataRequest(pydantic.BaseModel):
    # These fields are optional because None means "no changes; do not update".
    metadata: dict | None = None
    specs: Specs | None = None
    access_tags: list[str] | None = None
    # BACK-COMPAT: remove together with the access_blob helpers in _backcompat.py.
    access_blob: dict | None = pydantic.Field(
        default=None, deprecated=_ACCESS_BLOB_DEPRECATION
    )

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


class HyphenizedBaseModel(pydantic.BaseModel):
    # This model configuration allows aliases like "content-type"
    model_config = ConfigDict(alias_generator=lambda f: f.replace("_", "-"))


class PatchMetadataRequest(HyphenizedBaseModel):
    content_type: str

    # These fields are optional because None means "no changes; do not update".
    # Dict for merge-patch:
    metadata: list[JSONPatchAny] | dict | None = None

    # Specs for merge-patch. left_to_right mode is used to distinguish between
    # merge-patch List[asdict(Spec)] and json-patch List[Dict]
    specs: Specs | list[JSONPatchSpec] | None = Field(union_mode="left_to_right")

    # These fields are optional because None means "no changes; do not update".
    # List for merge-patch:
    # Define an alias to override parent class alias generator
    access_tags: list[JSONPatchAny] | list[str] | None = Field(
        alias="access_tags", default=None
    )
    # BACK-COMPAT: remove together with the access_blob helpers in _backcompat.py.
    # A legacy client patches the blob, so this is a patch of a Dict.
    access_blob: list[JSONPatchAny] | dict | None = Field(
        alias="access_blob", default=None, deprecated=_ACCESS_BLOB_DEPRECATION
    )

    @pydantic.field_validator("specs")
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


class PatchMetadataResponse(pydantic.BaseModel, Generic[ResourceLinksT]):
    id: str
    links: ArrayLinks | DataFrameLinks | SparseLinks
    # May be None if not altered
    metadata: dict | None
    data_sources: list[DataSource] | None
    access_tags: list[str] | None


SearchResponse = Response[
    list[Resource[NodeAttributes, dict, dict]], PaginationLinks, dict
]


class EnvelopeFormat(enum.StrEnum):
    json = "json"
    msgpack = "msgpack"


class EventType(enum.StrEnum):
    container_child_created = "container-child-created"
    container_child_metadata_updated = "container-child-metadata-updated"
    stream_closed = "stream-closed"


class DeliveryOutcome(enum.StrEnum):
    pending = "pending"
    success = "success"
    failed = "failed"


class ContainerChildCreatedEvent(pydantic.BaseModel):
    type: Literal[EventType.container_child_created] = EventType.container_child_created
    timestamp: AwareDatetime
    key: str
    path: list[str]
    structure_family: StructureFamily
    specs: list[dict[str, Any]]
    metadata: dict[str, Any]


class ContainerChildMetadataUpdatedEvent(pydantic.BaseModel):
    type: Literal[EventType.container_child_metadata_updated] = (
        EventType.container_child_metadata_updated
    )
    timestamp: AwareDatetime
    key: str
    path: list[str]
    specs: list[dict[str, Any]]
    metadata: dict[str, Any]


class StreamClosedEvent(pydantic.BaseModel):
    type: Literal[EventType.stream_closed] = EventType.stream_closed
    timestamp: AwareDatetime
    key: str
    path: list[str]


WebhookEvent = Annotated[
    ContainerChildCreatedEvent | ContainerChildMetadataUpdatedEvent | StreamClosedEvent,
    Field(discriminator="type"),
]


class WebhookRegistrationRequest(pydantic.BaseModel):
    url: AnyHttpUrl
    secret: str | None = None
    events: list[EventType] | None = None

    def check_https(self) -> None:
        """Raise ValueError if the URL scheme is not HTTPS.

        Not a field validator so callers (e.g. _noop_url_validator) can bypass it.
        """
        if self.url.scheme != "https":
            raise ValueError(
                "Webhook URL must use HTTPS to protect the HMAC signature in transit."
            )

    @field_validator("events", mode="before")
    @classmethod
    def normalize_empty_events(cls, v):
        if v == []:
            return None
        return v


class WebhookResponse(pydantic.BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    url: AnyHttpUrl
    events: list[EventType] | None
    active: bool
    time_created: AwareDatetime

    @field_validator("time_created", mode="before")
    @classmethod
    def ensure_tz(cls, v: datetime) -> datetime:
        if isinstance(v, datetime) and v.tzinfo is None:
            return v.replace(tzinfo=UTC)
        return v


class DeliveryResponse(pydantic.BaseModel):
    """Response containing webhook delivery information."""

    model_config = ConfigDict(
        from_attributes=True, validate_by_name=True, validate_by_alias=True
    )

    id: int
    webhook_id: int
    event_type: EventType
    payload: dict
    status_code: HTTPStatus | None
    attempts: int
    outcome: DeliveryOutcome
    error_detail: str | None
    time_created: AwareDatetime
    delivered_at: AwareDatetime | None

    @field_validator("time_created", "delivered_at", mode="before")
    @classmethod
    def ensure_tz(cls, v: datetime) -> datetime:
        if isinstance(v, datetime) and v.tzinfo is None:
            return v.replace(tzinfo=UTC)
        return v


NodeStructure.model_rebuild()
