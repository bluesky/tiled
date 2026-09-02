"""
Strawberry GraphQL schema for tiled graph.

Graph model:
  - Entity  — a named node with a type and arbitrary JSON properties
  - Link     — a directed, predicate-labeled edge between two entities
  - Namespace — a CURIE prefix -> URI mapping used to expand/compact terms
    (property keys and link predicates).

Query highlights:
    - entity / entities — fetch nodes
    - link / links      — fetch edges, filterable by subject, predicate, object
    - namespaces        — list registered CURIE prefixes
    - Entity.outgoing_links / incoming_links — graph traversal from a node

Mutations:
    - createEntity / createLink
    - deleteEntity (cascades to attached links) / deleteLink
    - upsertNamespace / deleteNamespace

Property keys and link predicates are expanded against the namespace
registry when written and compacted back to CURIEs when read, so a
prefix registered through `upsertNamespace` is resolved consistently.
"""

from __future__ import annotations

import logging
from typing import Optional

import strawberry
from graphql import GraphQLError
from strawberry.extensions import QueryDepthLimiter
from strawberry.scalars import JSON as StrawberryJSON
from strawberry.types import Info
from strawberry.types.unset import UNSET, UnsetType

from tiled.access_control.access_policies import NO_ACCESS
from tiled.queries import AccessBlobFilter
from tiled.type_aliases import AccessBlob

from .curie import compact_term, compact_value, expand_term, expand_value
from .orm import ENTITY_NODE_ACCESS_BLOB_ERROR
from .store import UNSET as STORE_UNSET
from .store import EntityRecord, GraphSQLAlchemyStore, LinkRecord

logger = logging.getLogger(__name__)

# Maximum nesting depth allowed in a single GraphQL query. The entity/link
# graph is recursively traversable (Entity.outgoingLinks -> Link.object ->
# Entity.outgoingLinks -> ...), so an unbounded query could force arbitrarily
# deep and expensive resolution. Introspection queries are exempt (the limiter
# ignores them by default), so the GraphiQL "Docs" panel is unaffected.
MAX_QUERY_DEPTH = 10

# ---------------------------------------------------------------------------
# JSON scalar — pass arbitrary dicts / lists / primitives through GraphQL
# ---------------------------------------------------------------------------

JSON = StrawberryJSON

# Helpers
# ---------------------------------------------------------------------------


def _store(info: Info) -> GraphSQLAlchemyStore:
    return info.context["store"]


async def _namespaces(info: Info) -> dict[str, str]:
    return await _store(info).list_namespaces()


class _PolicyNode:
    def __init__(self, access_blob: Optional[AccessBlob]):
        self.access_blob = access_blob or AccessBlob(tags=[])


async def _is_allowed(
    info: Info, access_blob: Optional[AccessBlob], scope: str
) -> bool:
    authn_scopes = info.context["authn_scopes"]
    if scope not in authn_scopes:
        return False
    policy = info.context.get("access_policy")
    if policy is None or not hasattr(policy, "allowed_scopes"):
        return True
    allowed = await policy.allowed_scopes(
        _PolicyNode(access_blob),
        info.context["principal"],
        info.context["authn_access_tags"],
        authn_scopes,
    )
    return scope in allowed


async def _assert_allowed(
    info: Info, access_blob: Optional[AccessBlob], scope: str
) -> None:
    if not await _is_allowed(info, access_blob, scope):
        raise GraphQLError("Not permitted")


async def _effective_access_blob(info: Info, record: EntityRecord) -> AccessBlob:
    """
    An entity that points to a catalog node (node_id set) delegates its
    access control to that node, rather than carrying its own access_blob
    (which is NULL in that case; see the entities_node_access_blob_*
    trigger in tiled.graph.store). Resolve whichever one is authoritative.
    """
    if record.node_id is not None:
        return await _store(info).get_node_access_blob(record.node_id) or AccessBlob(
            tags=[]
        )
    return record.access_blob or AccessBlob(tags=[])


def _assert_authn_scope(info: Info, scope: str) -> None:
    if scope not in info.context["authn_scopes"]:
        raise GraphQLError("Not permitted")


async def _policy_access_filters(info: Info, scope: str) -> object:
    """
    Return the access-policy filters for a listing query: either a list of
    AccessBlobFilter (possibly empty, meaning no restriction) or the
    NO_ACCESS sentinel. Callers pass the result straight to the store so
    filtering happens in SQL before LIMIT/OFFSET, rather than filtering a
    page of results after the fact (which would return fewer than `limit`
    rows even when more visible rows exist).
    """
    policy = info.context.get("access_policy")
    if policy is None or not hasattr(policy, "filters"):
        return []

    queries = await policy.filters(
        _PolicyNode(AccessBlob(tags=[])),
        info.context["principal"],
        info.context["authn_access_tags"],
        info.context["authn_scopes"],
        {scope},
    )
    if queries is NO_ACCESS:
        return NO_ACCESS
    for query in queries:
        if not isinstance(query, AccessBlobFilter):
            raise GraphQLError(
                f"Unsupported access-policy filter in graph queries: {type(query).__name__}"
            )
    return queries


def _access_blob_from_input(access_blob: dict) -> AccessBlob:
    if "user" in access_blob and set(access_blob) == {"user"}:
        return AccessBlob(username=access_blob["user"])
    if set(access_blob) <= {"tags"}:
        return AccessBlob(tags=access_blob.get("tags", []))
    raise GraphQLError(
        'access_blob must be either {"user": <username>} or '
        '{"tags": [<tag>, ...]}. '
        f"Received {access_blob!r}"
    )


async def _init_access_blob(
    info: Info, access_blob: Optional[AccessBlob]
) -> AccessBlob:
    policy = info.context.get("access_policy")
    if policy is not None and hasattr(policy, "init_node"):
        try:
            _, new_access_blob = await policy.init_node(
                info.context["principal"],
                info.context["authn_access_tags"],
                info.context["authn_scopes"],
                access_blob=access_blob,
            )
        except ValueError as exc:
            raise GraphQLError(f"Access policy rejects access blob: {exc}") from exc
        if not isinstance(new_access_blob, AccessBlob):
            raise TypeError("access policy must return an AccessBlob")
        return new_access_blob
    return access_blob or AccessBlob(tags=[])


async def _modify_access_blob(
    info: Info,
    current_access_blob: Optional[AccessBlob],
    requested_access_blob: Optional[AccessBlob],
) -> AccessBlob:
    policy = info.context.get("access_policy")
    if policy is not None and hasattr(policy, "modify_node"):
        try:
            _, new_access_blob = await policy.modify_node(
                _PolicyNode(current_access_blob),
                info.context["principal"],
                info.context["authn_access_tags"],
                info.context["authn_scopes"],
                requested_access_blob,
            )
        except ValueError as exc:
            raise GraphQLError(f"Access policy rejects access blob: {exc}") from exc
        if not isinstance(new_access_blob, AccessBlob):
            raise TypeError("access policy must return an AccessBlob")
        return new_access_blob
    return current_access_blob or AccessBlob(tags=[])


# ---------------------------------------------------------------------------
# Output types
# ---------------------------------------------------------------------------


@strawberry.type
class Entity:
    id: strawberry.ID
    node_id: Optional[int]
    entity_type: str
    name: str
    uri: Optional[str]
    properties: Optional[JSON]  # type: ignore[valid-type]
    created_at: str

    @strawberry.field(description="Links where this entity is the subject.")
    async def outgoing_links(
        self,
        info: Info,
        predicate: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list["Link"]:
        access_filters = await _policy_access_filters(info, "read:metadata")
        if access_filters is NO_ACCESS:
            return []
        namespaces = await _namespaces(info)
        expanded_predicate = expand_term(predicate, namespaces) if predicate else None
        records = await _store(info).find_links(
            subject_id=str(self.id),
            predicate=expanded_predicate,
            limit=limit,
            offset=offset,
            access_filters=access_filters or None,
        )
        return [_link_from_record(r, namespaces) for r in records]

    @strawberry.field(description="Links where this entity is the object.")
    async def incoming_links(
        self,
        info: Info,
        predicate: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list["Link"]:
        access_filters = await _policy_access_filters(info, "read:metadata")
        if access_filters is NO_ACCESS:
            return []
        namespaces = await _namespaces(info)
        expanded_predicate = expand_term(predicate, namespaces) if predicate else None
        records = await _store(info).find_links(
            object_id=str(self.id),
            predicate=expanded_predicate,
            limit=limit,
            offset=offset,
            access_filters=access_filters or None,
        )
        return [_link_from_record(r, namespaces) for r in records]


@strawberry.type
class Link:
    id: strawberry.ID
    subject_id: strawberry.ID
    predicate: str
    object_id: strawberry.ID
    properties: Optional[JSON]  # type: ignore[valid-type]
    access_blob: Optional[JSON]  # type: ignore[valid-type]
    created_at: str

    @strawberry.field
    async def subject(self, info: Info) -> Optional[Entity]:
        record = await _store(info).get_entity(str(self.subject_id))
        if record is None:
            return None
        access_blob = await _effective_access_blob(info, record)
        if not await _is_allowed(info, access_blob, "read:metadata"):
            return None
        return _entity_from_record(record, await _namespaces(info))

    @strawberry.field
    async def object(self, info: Info) -> Optional[Entity]:
        record = await _store(info).get_entity(str(self.object_id))
        if record is None:
            return None
        access_blob = await _effective_access_blob(info, record)
        if not await _is_allowed(info, access_blob, "read:metadata"):
            return None
        return _entity_from_record(record, await _namespaces(info))


@strawberry.type(
    description="A CURIE prefix -> URI mapping used to expand/compact terms."
)
class Namespace:
    prefix: str
    uri: str


# ---------------------------------------------------------------------------
# Record -> GQL type converters
# ---------------------------------------------------------------------------


def _entity_from_record(r: EntityRecord, namespaces: dict[str, str]) -> Entity:
    properties = compact_value(r.properties, namespaces) if r.properties else None
    return Entity(
        id=strawberry.ID(r.id),
        node_id=r.node_id,
        entity_type=r.entity_type,
        name=r.name,
        uri=r.uri,
        properties=properties,
        created_at=r.created_at.isoformat(),
    )


def _link_from_record(r: LinkRecord, namespaces: dict[str, str]) -> Link:
    properties = compact_value(r.properties, namespaces) if r.properties else None
    access_blob = (
        {"user": r.access_blob.username}
        if r.access_blob.username is not None
        else {"tags": r.access_blob.tags or []}
    )
    return Link(
        id=strawberry.ID(r.id),
        subject_id=strawberry.ID(r.subject_id),
        predicate=compact_term(r.predicate, namespaces),
        object_id=strawberry.ID(r.object_id),
        properties=properties,
        access_blob=access_blob,
        created_at=r.created_at.isoformat(),
    )


# ---------------------------------------------------------------------------
# Input types
# ---------------------------------------------------------------------------


@strawberry.input
class UpdateEntityInput:
    name: Optional[str] = None
    node_id: Optional[int] | UnsetType = UNSET
    uri: Optional[str] | UnsetType = UNSET
    entity_type: Optional[str] = None
    access_blob: Optional[JSON] | UnsetType = UNSET  # type: ignore[valid-type]


@strawberry.input
class UpdateLinkInput:
    predicate: Optional[str] | UnsetType = UNSET
    access_blob: Optional[JSON] | UnsetType = UNSET  # type: ignore[valid-type]


@strawberry.input
class CreateEntityInput:
    entity_type: str
    name: str
    node_id: Optional[int] = None
    uri: Optional[str] = None
    properties: Optional[JSON] = None  # type: ignore[valid-type]
    access_blob: Optional[JSON] = None  # type: ignore[valid-type]


@strawberry.input
class CreateLinkInput:
    subject_id: strawberry.ID
    predicate: str
    object_id: strawberry.ID
    properties: Optional[JSON] = None  # type: ignore[valid-type]
    access_blob: Optional[JSON] = None  # type: ignore[valid-type]


# ---------------------------------------------------------------------------
# Query / Mutation
# ---------------------------------------------------------------------------


@strawberry.type
class Query:
    @strawberry.field
    async def entity(self, info: Info, id: strawberry.ID) -> Optional[Entity]:
        record = await _store(info).get_entity(str(id))
        if record is None:
            return None
        access_blob = await _effective_access_blob(info, record)
        if not await _is_allowed(info, access_blob, "read:metadata"):
            return None
        return _entity_from_record(record, await _namespaces(info))

    @strawberry.field
    async def entities(
        self,
        info: Info,
        entity_type: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Entity]:
        access_filters = await _policy_access_filters(info, "read:metadata")
        if access_filters is NO_ACCESS:
            return []
        records = await _store(info).list_entities(
            entity_type=entity_type,
            limit=limit,
            offset=offset,
            access_filters=access_filters or None,
        )
        namespaces = await _namespaces(info)
        return [_entity_from_record(r, namespaces) for r in records]

    @strawberry.field(
        description=(
            "Resolve the internal catalog node id for a path of key "
            "segments (e.g. ['raw_dataset']), for use as CreateEntityInput's "
            "nodeId. Returns null if no such catalog node exists."
        )
    )
    async def catalog_node_id(self, info: Info, path: list[str]) -> Optional[int]:
        _assert_authn_scope(info, "read:metadata")
        return await _store(info).resolve_node_id(path)

    @strawberry.field
    async def link(self, info: Info, id: strawberry.ID) -> Optional[Link]:
        record = await _store(info).get_link(str(id))
        if not record or not await _is_allowed(
            info, record.access_blob, "read:metadata"
        ):
            return None
        return _link_from_record(record, await _namespaces(info))

    @strawberry.field(
        description="Find links, optionally filtered by subject, predicate, and/or object."
    )
    async def links(
        self,
        info: Info,
        subject_id: Optional[strawberry.ID] = None,
        predicate: Optional[str] = None,
        object_id: Optional[strawberry.ID] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Link]:
        access_filters = await _policy_access_filters(info, "read:metadata")
        if access_filters is NO_ACCESS:
            return []
        namespaces = await _namespaces(info)
        expanded_predicate = expand_term(predicate, namespaces) if predicate else None
        records = await _store(info).find_links(
            subject_id=str(subject_id) if subject_id else None,
            predicate=expanded_predicate,
            object_id=str(object_id) if object_id else None,
            limit=limit,
            offset=offset,
            access_filters=access_filters or None,
        )
        return [_link_from_record(r, namespaces) for r in records]

    @strawberry.field(description="List registered CURIE prefix -> URI mappings.")
    async def namespaces(self, info: Info) -> list[Namespace]:
        if "read:metadata" not in info.context["authn_scopes"]:
            return []
        mapping = await _namespaces(info)
        return [
            Namespace(prefix=prefix, uri=uri) for prefix, uri in sorted(mapping.items())
        ]


@strawberry.type
class Mutation:
    @strawberry.mutation
    async def create_entity(self, info: Info, input: CreateEntityInput) -> Entity:
        _assert_authn_scope(info, "write:metadata")
        namespaces = await _namespaces(info)
        if input.node_id is not None:
            if input.access_blob:
                raise GraphQLError(ENTITY_NODE_ACCESS_BLOB_ERROR)
            access_blob = None
        else:
            access_blob = await _init_access_blob(
                info,
                (
                    _access_blob_from_input(input.access_blob)
                    if input.access_blob is not None
                    else None
                ),
            )
        record = await _store(info).create_entity(
            entity_type=input.entity_type,
            name=input.name,
            node_id=input.node_id,
            uri=input.uri,
            properties=expand_value(input.properties or {}, namespaces),
            access_blob=access_blob,
        )
        logger.info(
            "Created entity type=%r name=%r id=%s",
            record.entity_type,
            record.name,
            record.id,
        )
        return _entity_from_record(record, namespaces)

    @strawberry.mutation
    async def create_link(self, info: Info, input: CreateLinkInput) -> Link:
        _assert_authn_scope(info, "write:metadata")
        namespaces = await _namespaces(info)
        subject = await _store(info).get_entity(str(input.subject_id))
        if not subject:
            raise GraphQLError(f"Subject entity '{input.subject_id}' not found")
        await _assert_allowed(
            info, await _effective_access_blob(info, subject), "write:metadata"
        )
        object_ = await _store(info).get_entity(str(input.object_id))
        if not object_:
            raise GraphQLError(f"Object entity '{input.object_id}' not found")
        await _assert_allowed(
            info, await _effective_access_blob(info, object_), "write:metadata"
        )
        access_blob = await _init_access_blob(
            info,
            (
                _access_blob_from_input(input.access_blob)
                if input.access_blob is not None
                else None
            ),
        )
        record = await _store(info).create_link(
            subject_id=str(input.subject_id),
            predicate=expand_term(input.predicate, namespaces),
            object_id=str(input.object_id),
            properties=expand_value(input.properties or {}, namespaces),
            access_blob=access_blob,
        )
        logger.info(
            "Created link %s -[%s]-> %s id=%s",
            record.subject_id[:8],
            record.predicate,
            record.object_id[:8],
            record.id,
        )
        return _link_from_record(record, namespaces)

    @strawberry.mutation(
        description="Delete an entity and all its attached links. Returns true if found."
    )
    async def delete_entity(self, info: Info, id: strawberry.ID) -> bool:
        record = await _store(info).get_entity(str(id))
        if not record:
            return False
        await _assert_allowed(
            info, await _effective_access_blob(info, record), "write:metadata"
        )
        deleted = await _store(info).delete_entity(str(id))
        if deleted:
            logger.info("Deleted entity id=%s", id)
        return deleted

    @strawberry.mutation(description="Update an entity's name, uri, or entity_type.")
    async def update_entity(
        self, info: Info, id: strawberry.ID, input: UpdateEntityInput
    ) -> Optional[Entity]:
        current = await _store(info).get_entity(str(id))
        if current is None:
            return None
        await _assert_allowed(
            info, await _effective_access_blob(info, current), "write:metadata"
        )
        effective_node_id = current.node_id if input.node_id is UNSET else input.node_id
        access_blob = UNSET
        if effective_node_id is not None:
            if input.access_blob is not UNSET and (input.access_blob or {}):
                raise GraphQLError(ENTITY_NODE_ACCESS_BLOB_ERROR)
            access_blob = None
        elif input.access_blob is not UNSET:
            requested_access_blob = (
                _access_blob_from_input(input.access_blob)
                if input.access_blob is not None
                else None
            )
            access_blob = await _modify_access_blob(
                info,
                current.access_blob,
                requested_access_blob,
            )
        elif input.node_id is not UNSET and current.access_blob is None:
            # Detaching from a node (node_id set to null) with no
            # access_blob supplied in the same call: the entity needs its
            # own access_blob now that it no longer delegates to a node.
            access_blob = await _init_access_blob(info, None)
        node_id = STORE_UNSET if input.node_id is UNSET else input.node_id
        uri = STORE_UNSET if input.uri is UNSET else input.uri
        record = await _store(info).update_entity(
            str(id),
            name=input.name,
            node_id=node_id,
            uri=uri,
            entity_type=input.entity_type,
            access_blob=access_blob,
        )
        if record:
            logger.info("Updated entity id=%s", id)
        return _entity_from_record(record, await _namespaces(info)) if record else None

    @strawberry.mutation(description="Delete a single link. Returns true if found.")
    async def delete_link(self, info: Info, id: strawberry.ID) -> bool:
        record = await _store(info).get_link(str(id))
        if not record:
            return False
        await _assert_allowed(info, record.access_blob, "write:metadata")
        deleted = await _store(info).delete_link(str(id))
        if deleted:
            logger.info("Deleted link id=%s", id)
        return deleted

    @strawberry.mutation(description="Update a link's predicate.")
    async def update_link(
        self, info: Info, id: strawberry.ID, input: UpdateLinkInput
    ) -> Optional[Link]:
        current = await _store(info).get_link(str(id))
        if current is None:
            return None
        await _assert_allowed(info, current.access_blob, "write:metadata")
        namespaces = await _namespaces(info)
        access_blob = UNSET
        if input.access_blob is not UNSET:
            requested_access_blob = (
                _access_blob_from_input(input.access_blob)
                if input.access_blob is not None
                else None
            )
            access_blob = await _modify_access_blob(
                info, current.access_blob, requested_access_blob
            )
        predicate = (
            STORE_UNSET
            if input.predicate is UNSET
            else expand_term(input.predicate, namespaces)
        )
        record = await _store(info).update_link(
            str(id),
            predicate=predicate,
            access_blob=access_blob,
        )
        if record:
            logger.info("Updated link id=%s predicate=%r", id, input.predicate)
        return _link_from_record(record, namespaces) if record else None

    @strawberry.mutation(
        description="Register or update a CURIE prefix -> URI mapping."
    )
    async def upsert_namespace(self, info: Info, prefix: str, uri: str) -> Namespace:
        _assert_authn_scope(info, "write:metadata")
        await _store(info).upsert_namespace(prefix, uri)
        logger.info("Upserted namespace prefix=%r uri=%r", prefix, uri)
        return Namespace(prefix=prefix, uri=uri)

    @strawberry.mutation(
        description="Delete a registered namespace. Returns true if found."
    )
    async def delete_namespace(self, info: Info, prefix: str) -> bool:
        _assert_authn_scope(info, "write:metadata")
        deleted = await _store(info).delete_namespace(prefix)
        if deleted:
            logger.info("Deleted namespace prefix=%r", prefix)
        return deleted


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

schema = strawberry.Schema(
    query=Query,
    mutation=Mutation,
    extensions=[lambda: QueryDepthLimiter(max_depth=MAX_QUERY_DEPTH)],
)
