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

from .curie import compact_term, compact_value, expand_term, expand_value
from .orm import ENTITY_NODE_ACCESS_BLOB_ERROR
from .store import UNSET as STORE_UNSET
from .store import EntityConflictError, EntityRecord, GraphSQLAlchemyStore, LinkRecord

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


async def _resolve_node_binding(
    info: Info,
    node_path_parts: Optional[list[str]],
) -> Optional[int]:
    """Resolve the catalog node (ID) an entity should bind to given node path

    The provided node path is resolved to the id of an internal node in the catalog
    (so clients never handle catalog integer ids). Returns None when node_path_parts
    is None (an unbound, external entity). Raises if the path names no existing node.
    """
    if node_path_parts is None:
        return None
    resolved = await _store(info).resolve_node_id(node_path_parts)
    if resolved is None:
        raise GraphQLError(
            f"No catalog node found at path {node_path_parts!r}.",
            extensions={"code": "NODE_NOT_FOUND"},
        )
    return resolved


class _PolicyNode:
    def __init__(self, access_blob: Optional[dict]):
        self.access_blob = access_blob or {}


async def _is_allowed(info: Info, access_blob: Optional[dict], scope: str) -> bool:
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


async def _assert_allowed(info: Info, access_blob: Optional[dict], scope: str) -> None:
    if not await _is_allowed(info, access_blob, scope):
        raise GraphQLError("Not permitted")


async def _effective_access_blob(info: Info, record: EntityRecord) -> Optional[dict]:
    """
    An entity that points to a catalog node (node_id set) delegates its
    access control to that node, rather than carrying its own access_blob
    (which is NULL in that case; see the entities_node_access_blob_*
    trigger in tiled.graph.store). Resolve whichever one is authoritative.
    """
    if record.node_id is not None:
        return await _store(info).get_node_access_blob(record.node_id)
    return record.access_blob


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
        _PolicyNode({}),
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


async def _init_access_blob(info: Info, access_blob: Optional[dict]) -> dict:
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
        return new_access_blob
    return access_blob or {}


async def _modify_access_blob(
    info: Info, current_access_blob: Optional[dict], requested_access_blob: dict
) -> dict:
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
        return new_access_blob
    return current_access_blob


# ---------------------------------------------------------------------------
# Output types
# ---------------------------------------------------------------------------


@strawberry.type
class Entity:
    id: strawberry.ID
    is_node_bound: bool
    kind: str
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
        is_node_bound=r.node_id is not None,
        kind=r.kind,
        name=r.name,
        uri=r.uri,
        properties=properties,
        created_at=r.created_at.isoformat(),
    )


def _link_from_record(r: LinkRecord, namespaces: dict[str, str]) -> Link:
    properties = compact_value(r.properties, namespaces) if r.properties else None
    return Link(
        id=strawberry.ID(r.id),
        subject_id=strawberry.ID(r.subject_id),
        predicate=compact_term(r.predicate, namespaces),
        object_id=strawberry.ID(r.object_id),
        properties=properties,
        access_blob=r.access_blob if r.access_blob else None,
        created_at=r.created_at.isoformat(),
    )


# ---------------------------------------------------------------------------
# Input types
# ---------------------------------------------------------------------------


@strawberry.input
class UpdateEntityInput:
    kind: Optional[str] = None
    name: Optional[str] = None
    # Re-bind to a catalog node by path (list of key segments), detach with an
    # explicit null, or leave the current binding unchanged by omitting it.
    node_path_parts: Optional[list[str]] | UnsetType = UNSET
    uri: Optional[str] | UnsetType = UNSET
    access_blob: Optional[JSON] | UnsetType = UNSET  # type: ignore[valid-type]


@strawberry.input
class UpdateLinkInput:
    predicate: Optional[str] | UnsetType = UNSET
    access_blob: Optional[JSON] | UnsetType = UNSET  # type: ignore[valid-type]


@strawberry.input
class CreateEntityInput:
    kind: str
    name: str
    # Bind this entity to a catalog node by its path of key segments
    # (e.g. ["a", "b"]). Resolved to the internal node id server-side, so
    # clients never handle the catalog's integer ids. Omit for an entity that
    # is not tied to a node in this server's catalog.
    node_path_parts: Optional[list[str]] = None
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
        kind: Optional[str] = None,
        name: Optional[str] = None,
        node_path_parts: Optional[list[str]] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Entity]:
        access_filters = await _policy_access_filters(info, "read:metadata")
        if access_filters is NO_ACCESS:
            return []
        node_id = None
        if node_path_parts is not None:
            node_id = await _store(info).resolve_node_id(node_path_parts)
            if node_id is None:
                # No such catalog node -> it can have no bound entities.
                return []
        records = await _store(info).list_entities(
            kind=kind,
            name=name,
            node_id=node_id,
            limit=limit,
            offset=offset,
            access_filters=access_filters or None,
        )
        namespaces = await _namespaces(info)
        return [_entity_from_record(r, namespaces) for r in records]

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
        node_id = await _resolve_node_binding(info, input.node_path_parts)
        if node_id is not None:
            if input.access_blob:
                raise GraphQLError(ENTITY_NODE_ACCESS_BLOB_ERROR)
            access_blob = None
        else:
            access_blob = await _init_access_blob(info, input.access_blob)
        try:
            record = await _store(info).create_entity(
                kind=input.kind,
                name=input.name,
                node_id=node_id,
                uri=input.uri,
                properties=expand_value(input.properties or {}, namespaces),
                access_blob=access_blob,
            )
        except EntityConflictError as exc:
            raise GraphQLError(str(exc), extensions={"code": "ENTITY_EXISTS"})
        logger.info(
            "Created entity kind=%r name=%r id=%s",
            record.kind,
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
        access_blob = await _init_access_blob(info, input.access_blob)
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

    @strawberry.mutation(description="Update an entity's name, uri, or kind.")
    async def update_entity(
        self, info: Info, id: strawberry.ID, input: UpdateEntityInput
    ) -> Optional[Entity]:
        current = await _store(info).get_entity(str(id))
        if current is None:
            return None
        await _assert_allowed(
            info, await _effective_access_blob(info, current), "write:metadata"
        )
        # Resolve the requested node binding into a tri-state on the internal
        # node id: UNSET (leave), None (detach), or an int (bind).
        # node_path_parts is resolved to the id, so the store never sees a path.
        if input.node_path_parts is UNSET:
            node_id = STORE_UNSET
        elif input.node_path_parts is None:
            node_id = None
        else:
            node_id = await _resolve_node_binding(info, input.node_path_parts)
        node_binding_changed = node_id is not STORE_UNSET
        effective_node_id = current.node_id if node_id is STORE_UNSET else node_id
        access_blob = UNSET
        if effective_node_id is not None:
            if input.access_blob is not UNSET and (input.access_blob or {}):
                raise GraphQLError(ENTITY_NODE_ACCESS_BLOB_ERROR)
            access_blob = None
        elif input.access_blob is not UNSET:
            requested_access_blob = input.access_blob or {}
            access_blob = await _modify_access_blob(
                info, current.access_blob, requested_access_blob
            )
        elif node_binding_changed and current.access_blob is None:
            # Detaching from a node (node_path_parts set to null) with no
            # access_blob supplied in the same call: the entity needs its
            # own access_blob now that it no longer delegates to a node.
            access_blob = await _init_access_blob(info, None)
        uri = STORE_UNSET if input.uri is UNSET else input.uri
        try:
            record = await _store(info).update_entity(
                str(id),
                kind=input.kind,
                name=input.name,
                node_id=node_id,
                uri=uri,
                access_blob=access_blob,
            )
        except EntityConflictError as exc:
            raise GraphQLError(str(exc), extensions={"code": "ENTITY_EXISTS"})
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
            requested_access_blob = input.access_blob or {}
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
