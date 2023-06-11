from __future__ import annotations

import base64
import copy
import enum
import uuid
from datetime import datetime
from typing import Any, Dict, Generic, List, Optional, TypeVar, Union

import pydantic
import pydantic.dataclasses
import pydantic.errors
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
    data_sources = "data_sources"
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
    label: pydantic.constr(max_length=255)
    url: pydantic.AnyUrl

    @classmethod
    def from_json(cls, json_doc):
        return cls(label=json_doc["label"], url=json_doc["url"])


class Spec(pydantic.BaseModel, extra=pydantic.Extra.forbid, frozen=True):
    name: pydantic.constr(max_length=255)
    version: Optional[pydantic.constr(max_length=255)]


References = pydantic.conlist(ReferenceDocument, max_items=20)
# Wait for fix https://github.com/pydantic/pydantic/issues/3957
# Specs = pydantic.conlist(Spec, max_items=20, unique_items=True)
Specs = pydantic.conlist(Spec, max_items=20)


class Asset(pydantic.BaseModel):
    data_uri: str
    is_directory: bool

    @classmethod
    def from_orm(cls, orm):
        return cls(data_uri=orm.data_uri, is_directory=orm.is_directory)


class Management(str, enum.Enum):
    external = "external"
    immutable = "immutable"
    locked = "locked"
    writable = "writable"


class Revision(pydantic.BaseModel):
    revision_number: int
    metadata: dict
    specs: Specs
    references: References
    time_updated: datetime

    @classmethod
    def from_orm(cls, orm):
        return cls(
            revision_number=orm.revision_number,
            metadata=orm.metadata_,
            specs=orm.specs,
            references=orm.references,
            time_updated=orm.time_updated,
        )


class DataSource(pydantic.BaseModel):
    structure: Optional[
        Union[ArrayStructure, DataFrameStructure, NodeStructure, SparseStructure]
    ] = None
    mimetype: Optional[str] = None
    parameters: dict = {}
    assets: List[Asset] = []
    management: Management = Management.writable

    @classmethod
    def from_orm(cls, orm):
        # if isinstance(orm.structure, DataFrameStructure):
        if "meta" in orm.structure.get("micro", {}):
            structure = copy.deepcopy(orm.structure)
            structure["micro"]["meta"] = base64.b64decode(structure["micro"]["meta"])
            structure["micro"]["divisions"] = base64.b64decode(
                structure["micro"]["divisions"]
            )
        else:
            structure = orm.structure
        return cls(
            structure=structure,
            mimetype=orm.mimetype,
            parameters=orm.parameters,
            assets=[Asset.from_orm(asset) for asset in orm.assets],
            management=orm.management,
        )


class NodeAttributes(pydantic.BaseModel):
    ancestors: List[str]
    structure_family: Optional[StructureFamily]
    specs: Optional[Specs]
    metadata: Optional[Dict]  # free-form, user-specified dict
    structure: Optional[
        Union[ArrayStructure, DataFrameStructure, NodeStructure, SparseStructure]
    ]
    sorting: Optional[List[SortingItem]]
    references: Optional[References]
    data_sources: Optional[List[DataSource]]


class Node(NodeAttributes):
    # In the HTTP response, we place the key *outside* the other attributes,
    # as "id". This was inspired by JSON API, and for now we are sticking
    # with it.
    #
    # But for passing the Node around internally, it is useful to have the
    # key included in the model.
    key: str
    access_policy: Any
    _node: Any = pydantic.PrivateAttr()
    _context: Any = pydantic.PrivateAttr()

    def __init__(self, node, context, **data):
        super().__init__(**data)
        self._node = node
        self._context = context

    @classmethod
    def from_orm(cls, orm, context, *, access_policy, sorting=None):
        sorting = sorting or []
        # In the Python API we encode sorting as (key, direction).
        # This order-based "record" notion does not play well with OpenAPI.
        # In the HTTP API, therefore, we use {"key": key, "direction": direction}.
        if sorting and isinstance(sorting[0], tuple):
            sorting = [SortingItem(key=item[0], direction=item[1]) for item in sorting]
        if len(orm.data_sources) > 1:
            # TODO Handle multiple data sources
            raise NotImplementedError
        if orm.data_sources:
            structure = copy.deepcopy(
                DataSource.from_orm(orm.data_sources[0]).structure
            )
        else:
            structure = None
        return cls(
            key=orm.key,
            ancestors=orm.ancestors,
            metadata=orm.metadata_,
            structure_family=orm.structure_family,
            structure=structure,
            specs=orm.specs,
            references=orm.references,
            sorting=sorting or [],
            data_sources=[DataSource.from_orm(ds) for ds in orm.data_sources],
            time_created=orm.time_created,
            time_updated=orm.time_updated,
            node=orm,
            context=context,
            access_policy=access_policy,
        )

    def microstructure(self):
        return getattr(self.structure, "micro", None)

    def macrostructure(self):
        return getattr(self.structure, "macro", None)

    async def revisions(self, offset, limit):
        async with self._context.session() as db:
            from sqlalchemy import select

            from tiled.catalog import orm

            revision_orms = (
                await db.execute(
                    select(orm.Revisions)
                    .where(orm.Revisions.node_id == self._node.id)
                    .offset(offset)
                    .limit(limit)
                )
            ).all()
            return [Revision.from_orm(o[0]) for o in revision_orms]

    async def delete_revision(self, number):
        async with self._context.session() as db:
            # TODO Abstract this from FastAPI?
            from fastapi import HTTPException
            from sqlalchemy import delete

            from tiled.catalog import orm

            result = await db.execute(
                delete(orm.Revisions)
                .where(orm.Revisions.node_id == self._node.id)
                .where(orm.Revisions.revision_number == number)
            )
            if result.rowcount == 0:
                raise HTTPException(
                    status_code=404,
                    detail=f"No revision {number} for node {self._node.id}",
                )
            if result.rowcount > 1:
                assert (
                    False
                ), f"Deletion would affect {result.rowcount} rows; rolling back"
            await db.commit()

    async def update_metadata(self, metadata=None, specs=None, references=None):
        values = {}
        if metadata is not None:
            # Trailing underscore in 'metadata_' avoids collision with
            # SQLAlchemy reserved word 'metadata'.
            values["metadata_"] = metadata
        if specs is not None:
            values["specs"] = [s.dict() for s in specs]
        if references is not None:
            values["references"] = [r.dict() for r in references]
        async with self._context.session() as db:
            from sqlalchemy import func, select, update

            from tiled.catalog import orm

            current = (
                await db.execute(select(orm.Node).where(orm.Node.id == self._node.id))
            ).scalar_one()
            next_revision_number = 1 + (
                (
                    await db.execute(
                        select(func.max(orm.Revisions.revision_number)).where(
                            orm.Revisions.node_id == self._node.id
                        )
                    )
                ).scalar_one()
                or 0
            )
            revision = orm.Revisions(
                metadata_=current.metadata_,
                specs=current.specs,
                references=current.references,
                node_id=current.id,
                revision_number=next_revision_number,
            )
            db.add(revision)
            await db.execute(
                update(orm.Node).where(orm.Node.id == self._node.id).values(**values)
            )
            await db.commit()


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
    id: Optional[str] = None
    structure_family: StructureFamily
    metadata: Dict = {}
    data_sources: List[DataSource] = []
    specs: Specs = []
    references: References = []

    # Wait for fix https://github.com/pydantic/pydantic/issues/3957
    # to do this with `unique_items` parameters to `pydantic.constr`.
    @pydantic.validator("specs", always=True)
    def specs_uniqueness_validator(cls, v):
        if v is None:
            return None
        for i, value in enumerate(v, start=1):
            if value in v[i:]:
                raise pydantic.errors.ListUniqueItemsError()
        return v


class PostMetadataResponse(pydantic.BaseModel, Generic[ResourceLinksT]):
    id: str
    links: Union[ArrayLinks, DataFrameLinks, SparseLinks]
    metadata: Dict
    data_sources: List[DataSource]


class PutMetadataResponse(pydantic.BaseModel, Generic[ResourceLinksT]):
    id: str
    links: Union[ArrayLinks, DataFrameLinks, SparseLinks]
    # May be None if not altered
    metadata: Optional[Dict]
    data_sources: Optional[List[DataSource]]


class DistinctValueInfo(pydantic.BaseModel):
    value: Any
    count: Optional[int]


class GetDistinctResponse(pydantic.BaseModel):
    metadata: Optional[Dict[str, List[DistinctValueInfo]]]
    structure_families: Optional[List[DistinctValueInfo]]
    specs: Optional[List[DistinctValueInfo]]


class PutMetadataRequest(pydantic.BaseModel):
    # These fields are optional because None means "no changes; do not update".
    metadata: Optional[Dict]
    specs: Optional[Specs]
    references: Optional[References]

    # Wait for fix https://github.com/pydantic/pydantic/issues/3957
    # to do this with `unique_items` parameters to `pydantic.constr`.
    @pydantic.validator("specs", always=True)
    def specs_uniqueness_validator(cls, v):
        if v is None:
            return None
        for i, value in enumerate(v, start=1):
            if value in v[i:]:
                raise pydantic.errors.ListUniqueItemsError()
        return v


NodeStructure.update_forward_refs()
