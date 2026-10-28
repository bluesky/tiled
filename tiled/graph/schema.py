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

Access control: entities and links carry access tags (the same tags that
catalog nodes use). An entity that points to a catalog node (nodeId set)
carries no tags of its own; it assumes the access tags of the referenced
node.
"""

from __future__ import annotations

import logging
from typing import Optional

import strawberry
from graphql import GraphQLError
from strawberry.dataloader import DataLoader
from strawberry.extensions import QueryDepthLimiter
from strawberry.scalars import JSON as StrawberryJSON
from strawberry.types import Info
from strawberry.types.unset import UNSET, UnsetType

from tiled.access_control.access_policies import NO_ACCESS
from tiled.access_control.protocols import normalize_access_tags
from tiled.queries import AccessTagsFilter
from tiled.type_aliases import AccessTags

from .curie import Namespaces, compact_term, compact_value, expand_term, expand_value
from .orm import ENTITY_NODE_ACCESS_TAGS_ERROR
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


class _RequestCache:
    """
    Per-request memo of lookups that every resolver in a query would
    otherwise repeat.

    A single GraphQL request fans out into one resolver call per field per
    node (`entities -> outgoingLinks -> object -> outgoingLinks -> ...`), and
    each of those needs the namespace registry, the principal's access
    filters, and the endpoint entities of its links. Without memoization that
    is O(nodes x depth) round-trips to the database for data that cannot
    change within the request. The cache lives on `info.context`, which the
    router builds fresh for every request, so nothing leaks across requests.

    Entity and node-tag lookups go through `DataLoader`s: resolvers that run
    concurrently within one event-loop tick (e.g. `object` for every link in a
    page) are collected and served by a single batched store query.
    """

    __slots__ = (
        "_store",
        "_namespaces",
        "_access_filters",
        "entities",
        "node_tags",
        "links",
    )

    def __init__(self, store: GraphSQLAlchemyStore):
        self._store = store
        self._namespaces: Optional[Namespaces] = None
        self._access_filters: dict[str, object] = {}
        self.entities: DataLoader[str, Optional[EntityRecord]] = DataLoader(
            load_fn=self._load_entities
        )
        self.node_tags: DataLoader[int, Optional[frozenset[str]]] = DataLoader(
            load_fn=self._load_node_tags
        )
        # Keyed on (endpoint, endpoint_id, predicate, limit, offset, scope):
        # the batch function groups keys that share everything but the
        # endpoint id and issues one query per group. The scope selects the
        # memoized access filters (see _policy_access_filters).
        self.links: DataLoader[tuple, list[LinkRecord]] = DataLoader(
            load_fn=self._load_links
        )

    async def namespaces(self) -> Namespaces:
        if self._namespaces is None:
            self._namespaces = Namespaces(await self._store.list_namespaces())
        return self._namespaces

    def invalidate_namespaces(self) -> None:
        "Drop the memoized registry after a mutation changes it."
        self._namespaces = None

    async def _load_entities(self, ids: list[str]) -> list[Optional[EntityRecord]]:
        found = await self._store.get_entities(ids)
        return [found.get(id_) for id_ in ids]

    async def _load_node_tags(
        self, node_ids: list[int]
    ) -> list[Optional[frozenset[str]]]:
        found = await self._store.get_node_access_tags_batch(node_ids)
        return [found.get(node_id) for node_id in node_ids]

    async def _load_links(self, keys: list[tuple]) -> list[list[LinkRecord]]:
        # Group by every component except the endpoint id, so that the links
        # of all nodes expanded with the same arguments come back in one
        # query per argument combination.
        groups: dict[tuple, list[str]] = {}
        for endpoint, endpoint_id, predicate, limit, offset, scope in keys:
            groups.setdefault((endpoint, predicate, limit, offset, scope), []).append(
                endpoint_id
            )
        results: dict[tuple, dict[str, list[LinkRecord]]] = {}
        for group_key, endpoint_ids in groups.items():
            endpoint, predicate, limit, offset, scope = group_key
            results[group_key] = await self._store.find_links_batch(
                endpoint=endpoint,
                endpoint_ids=endpoint_ids,
                predicate=predicate,
                limit=limit,
                offset=offset,
                access_filters=self._access_filters[scope] or None,
            )
        return [
            results[(endpoint, predicate, limit, offset, scope)].get(endpoint_id, [])
            for endpoint, endpoint_id, predicate, limit, offset, scope in keys
        ]


def new_request_cache(store: GraphSQLAlchemyStore) -> _RequestCache:
    """
    Build the per-request cache. The router calls this when it assembles the
    context for an HTTP request and stores the result under
    `context["request_cache"]`, so every resolver in that request shares one
    cache and nothing persists beyond it.
    """
    return _RequestCache(store)


def _cache(info: Info) -> _RequestCache:
    """
    The cache for the current request.

    Normally provided by the router under `context["request_cache"]`. A
    context built without one (e.g. `schema.execute` called directly) gets a
    cache created on first use and kept on the context, which then lives as
    long as that context object does.
    """
    cache = info.context.get("request_cache")
    if cache is None:
        cache = info.context["request_cache"] = _RequestCache(_store(info))
    return cache


async def _namespaces(info: Info) -> Namespaces:
    return await _cache(info).namespaces()


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
    def __init__(self, access_tags: Optional[AccessTags]):
        self.access_tags = normalize_access_tags(access_tags or ())


async def _is_allowed(
    info: Info, access_tags: Optional[AccessTags], scope: str
) -> bool:
    authn_scopes = info.context["authn_scopes"]
    if scope not in authn_scopes:
        return False
    policy = info.context.get("access_policy")
    if policy is None or not hasattr(policy, "allowed_scopes"):
        return True
    allowed = await policy.allowed_scopes(
        _PolicyNode(access_tags),
        info.context["principal"],
        info.context["authn_access_tags"],
        authn_scopes,
    )
    return scope in allowed


async def _assert_allowed(
    info: Info, access_tags: Optional[AccessTags], scope: str
) -> None:
    if not await _is_allowed(info, access_tags, scope):
        raise GraphQLError("Not permitted")


async def _assert_node_write(info: Info, node_id: Optional[int]) -> None:
    """
    Require write:metadata on the catalog node an entity is being bound to.

    Binding an entity to a node attaches graph metadata to that node and
    delegates the entity's access control to it, so it must require the same
    permission as writing the node's own metadata. Without this check any
    principal holding the global write:metadata scope could attach (or, via the
    uniqueness constraint, squat) entities on nodes they cannot write. A None
    node_id (external entity) is a no-op.
    """
    if node_id is None:
        return
    node_tags = await _store(info).get_node_access_tags(node_id)
    await _assert_allowed(
        info, normalize_access_tags(node_tags or ()), "write:metadata"
    )


async def _effective_access_tags(info: Info, record: EntityRecord) -> AccessTags:
    """
    An entity that points to a catalog node (node_id set) delegates its
    access control to that node, rather than carrying its own access tags
    (it has none in that case; see the entities_node_access_tags_association_* triggers
    in tiled.graph.orm). Resolve whichever is authoritative.
    """
    if record.node_id is not None:
        node_tags = await _cache(info).node_tags.load(record.node_id)
        return normalize_access_tags(node_tags or ())
    return normalize_access_tags(record.access_tags or ())


def _assert_authn_scope(info: Info, scope: str) -> None:
    if scope not in info.context["authn_scopes"]:
        raise GraphQLError("Not permitted")


async def _policy_access_filters(info: Info, scope: str) -> object:
    """
    Return the access-policy filters for a listing query: either a list of
    AccessTagsFilter (possibly empty, meaning no restriction) or the
    NO_ACCESS sentinel. Callers pass the result straight to the store so
    filtering happens in SQL before LIMIT/OFFSET, rather than filtering a
    page of results after the fact (which would return fewer than `limit`
    rows even when more visible rows exist).

    The result depends only on the principal and the scope, both fixed for
    the request, so it is computed once per scope and memoized: nested
    `outgoingLinks`/`incomingLinks` resolvers would otherwise re-run the
    policy and the tag-id resolution query for every node they expand.
    """
    cache = _cache(info)
    if scope in cache._access_filters:
        return cache._access_filters[scope]

    policy = info.context.get("access_policy")
    if policy is None or not hasattr(policy, "filters"):
        queries: object = []
    else:
        queries = await policy.filters(
            _PolicyNode(normalize_access_tags()),
            info.context["principal"],
            info.context["authn_access_tags"],
            info.context["authn_scopes"],
            {scope},
        )
        if queries is not NO_ACCESS:
            for query in queries:
                if not isinstance(query, AccessTagsFilter):
                    raise GraphQLError(
                        "Unsupported access-policy filter in graph queries: "
                        f"{type(query).__name__}"
                    )
                query.tag_ids = await _store(info).resolve_access_tag_ids_for_read(
                    query.tags
                )
    cache._access_filters[scope] = queries
    return queries


def _access_tags_from_input(access_tags: list[str]) -> AccessTags:
    try:
        return normalize_access_tags(access_tags)
    except TypeError as exc:
        raise GraphQLError(
            "accessTags must be a list of tag names, e.g. "
            f'["tag1", "tag2"]. Received {access_tags!r}'
        ) from exc


async def _init_access_tags(
    info: Info, access_tags: Optional[AccessTags]
) -> AccessTags:
    policy = info.context.get("access_policy")
    if policy is not None and hasattr(policy, "init_node"):
        try:
            _, new_access_tags = await policy.init_node(
                info.context["principal"],
                info.context["authn_access_tags"],
                info.context["authn_scopes"],
                access_tags=access_tags,
            )
        except ValueError as exc:
            raise GraphQLError(f"Access policy rejects access tags: {exc}") from exc
        if not isinstance(new_access_tags, frozenset):
            raise TypeError("access policy must return a frozenset of access tags")
        return new_access_tags
    return access_tags if access_tags is not None else normalize_access_tags()


async def _modify_access_tags(
    info: Info,
    current_access_tags: Optional[AccessTags],
    requested_access_tags: Optional[AccessTags],
) -> AccessTags:
    policy = info.context.get("access_policy")
    if policy is not None and hasattr(policy, "modify_node"):
        try:
            _, new_access_tags = await policy.modify_node(
                _PolicyNode(current_access_tags),
                info.context["principal"],
                info.context["authn_access_tags"],
                info.context["authn_scopes"],
                requested_access_tags,
            )
        except ValueError as exc:
            raise GraphQLError(f"Access policy rejects access tags: {exc}") from exc
        if not isinstance(new_access_tags, frozenset):
            raise TypeError("access policy must return a frozenset of access tags")
        return new_access_tags
    if requested_access_tags is not None:
        return requested_access_tags
    return normalize_access_tags(current_access_tags or ())


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
        return await _load_links(
            info, "subject_id", str(self.id), predicate, limit, offset
        )

    @strawberry.field(description="Links where this entity is the object.")
    async def incoming_links(
        self,
        info: Info,
        predicate: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list["Link"]:
        return await _load_links(
            info, "object_id", str(self.id), predicate, limit, offset
        )


async def _load_links(
    info: Info,
    endpoint: str,
    endpoint_id: str,
    predicate: Optional[str],
    limit: int,
    offset: int,
) -> list["Link"]:
    """
    Resolve one page of an entity's links at one endpoint, through the
    request's link loader so that expanding the links of every entity on a
    page costs one query per distinct (endpoint, predicate, page) rather than
    one per entity.
    """
    scope = "read:metadata"
    access_filters = await _policy_access_filters(info, scope)
    if access_filters is NO_ACCESS:
        return []
    namespaces = await _namespaces(info)
    expanded_predicate = expand_term(predicate, namespaces) if predicate else None
    records = await _cache(info).links.load(
        (endpoint, endpoint_id, expanded_predicate, limit, offset, scope)
    )
    return [_link_from_record(r, namespaces) for r in records]


@strawberry.type
class Link:
    id: strawberry.ID
    subject_id: strawberry.ID
    predicate: str
    object_id: strawberry.ID
    properties: Optional[JSON]  # type: ignore[valid-type]
    access_tags: list[str]
    created_at: str

    @strawberry.field
    async def subject(self, info: Info) -> Optional[Entity]:
        return await _load_visible_entity(info, str(self.subject_id))

    @strawberry.field
    async def object(self, info: Info) -> Optional[Entity]:
        return await _load_visible_entity(info, str(self.object_id))


async def _load_visible_entity(info: Info, id: str) -> Optional[Entity]:
    """
    Resolve a link endpoint, or None if it does not exist or the principal may
    not read it. Goes through the request's entity loader so that the endpoints
    of every link in a page are fetched with one batched query rather than one
    query per link.
    """
    record = await _cache(info).entities.load(id)
    if record is None:
        return None
    access_tags = await _effective_access_tags(info, record)
    if not await _is_allowed(info, access_tags, "read:metadata"):
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


def _entity_from_record(r: EntityRecord, namespaces: Namespaces) -> Entity:
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


def _link_from_record(r: LinkRecord, namespaces: Namespaces) -> Link:
    properties = compact_value(r.properties, namespaces) if r.properties else None
    return Link(
        id=strawberry.ID(r.id),
        subject_id=strawberry.ID(r.subject_id),
        predicate=compact_term(r.predicate, namespaces),
        object_id=strawberry.ID(r.object_id),
        properties=properties,
        access_tags=sorted(r.access_tags),
        created_at=r.created_at.isoformat(),
    )


# ---------------------------------------------------------------------------
# Input types
# ---------------------------------------------------------------------------


@strawberry.input
class UpdateEntityInput:
    name: Optional[str] = None
    # Re-bind to a catalog node by path (list of key segments), detach with an
    # explicit null, or leave the current binding unchanged by omitting it.
    node_path_parts: Optional[list[str]] | UnsetType = UNSET
    uri: Optional[str] | UnsetType = UNSET
    kind: Optional[str] = None
    access_tags: Optional[list[str]] | UnsetType = UNSET


@strawberry.input
class UpdateLinkInput:
    predicate: Optional[str] | UnsetType = UNSET
    access_tags: Optional[list[str]] | UnsetType = UNSET


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
    access_tags: Optional[list[str]] = None


@strawberry.input
class CreateLinkInput:
    subject_id: strawberry.ID
    predicate: str
    object_id: strawberry.ID
    properties: Optional[JSON] = None  # type: ignore[valid-type]
    access_tags: Optional[list[str]] = None


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
        access_tags = await _effective_access_tags(info, record)
        if not await _is_allowed(info, access_tags, "read:metadata"):
            return None
        return _entity_from_record(record, await _namespaces(info))

    @strawberry.field
    async def entities(
        self,
        info: Info,
        kind: Optional[str] = None,
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
            info, normalize_access_tags(record.access_tags), "read:metadata"
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
        # Binding to a node requires write access on that node, not merely the
        # global write:metadata scope.
        await _assert_node_write(info, node_id)
        if node_id is not None:
            if input.access_tags:
                raise GraphQLError(ENTITY_NODE_ACCESS_TAGS_ERROR)
            access_tags = None
        else:
            access_tags = await _init_access_tags(
                info,
                (
                    _access_tags_from_input(input.access_tags)
                    if input.access_tags is not None
                    else None
                ),
            )
        try:
            record = await _store(info).create_entity(
                kind=input.kind,
                name=input.name,
                node_id=node_id,
                uri=input.uri,
                properties=expand_value(input.properties or {}, namespaces),
                access_tags=access_tags,
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

    @strawberry.mutation(
        description=(
            "Get-or-create an entity bound to a catalog node, keyed on "
            "(node, kind, name). Idempotent: returns the existing entity if "
            "one already matches, else creates it."
        )
    )
    async def upsert_entity(self, info: Info, input: CreateEntityInput) -> Entity:
        _assert_authn_scope(info, "write:metadata")
        namespaces = await _namespaces(info)
        node_id = await _resolve_node_binding(info, input.node_path_parts)
        await _assert_node_write(info, node_id)
        if node_id is None:
            # Upsert has no natural key for external entities (node_id NULL is
            # distinct in the unique index), so it would silently create
            # duplicates. Require a node binding and steer callers to
            # createEntity for free-standing entities.
            raise GraphQLError(
                "upsertEntity requires nodePathParts; use createEntity for "
                "external (node-unbound) entities.",
                extensions={"code": "NODE_REQUIRED"},
            )
        if input.access_tags:
            raise GraphQLError(ENTITY_NODE_ACCESS_TAGS_ERROR)
        record = await _store(info).upsert_entity(
            kind=input.kind,
            name=input.name,
            node_id=node_id,
            uri=input.uri,
            properties=expand_value(input.properties or {}, namespaces),
        )
        logger.info(
            "Upserted entity kind=%r name=%r id=%s",
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
            info, await _effective_access_tags(info, subject), "write:metadata"
        )
        object_ = await _store(info).get_entity(str(input.object_id))
        if not object_:
            raise GraphQLError(f"Object entity '{input.object_id}' not found")
        await _assert_allowed(
            info, await _effective_access_tags(info, object_), "write:metadata"
        )
        access_tags = await _init_access_tags(
            info,
            (
                _access_tags_from_input(input.access_tags)
                if input.access_tags is not None
                else None
            ),
        )
        record = await _store(info).create_link(
            subject_id=str(input.subject_id),
            predicate=expand_term(input.predicate, namespaces),
            object_id=str(input.object_id),
            properties=expand_value(input.properties or {}, namespaces),
            access_tags=access_tags,
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
            info, await _effective_access_tags(info, record), "write:metadata"
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
            info, await _effective_access_tags(info, current), "write:metadata"
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
        # Re-binding to a (different) node requires write access on that target
        # node, in addition to write access on the entity as it stands now.
        if node_binding_changed and node_id is not None:
            await _assert_node_write(info, node_id)
        effective_node_id = current.node_id if node_id is STORE_UNSET else node_id
        access_tags = UNSET
        if effective_node_id is not None:
            if input.access_tags is not UNSET and (input.access_tags or []):
                raise GraphQLError(ENTITY_NODE_ACCESS_TAGS_ERROR)
            access_tags = None
        elif input.access_tags is not UNSET:
            requested_access_tags = (
                _access_tags_from_input(input.access_tags)
                if input.access_tags is not None
                else None
            )
            access_tags = await _modify_access_tags(
                info,
                (
                    normalize_access_tags(current.access_tags)
                    if current.access_tags is not None
                    else None
                ),
                requested_access_tags,
            )
        elif node_binding_changed and current.access_tags is None:
            # Detaching from a node (node_path_parts set to null) with no
            # access tags supplied in the same call: the entity needs its
            # own access tags now that it no longer delegates to a node.
            access_tags = await _init_access_tags(info, None)
        uri = STORE_UNSET if input.uri is UNSET else input.uri
        try:
            record = await _store(info).update_entity(
                str(id),
                name=input.name,
                node_id=node_id,
                uri=uri,
                kind=input.kind,
                access_tags=STORE_UNSET if access_tags is UNSET else access_tags,
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
        await _assert_allowed(
            info, normalize_access_tags(record.access_tags), "write:metadata"
        )
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
        await _assert_allowed(
            info, normalize_access_tags(current.access_tags), "write:metadata"
        )
        namespaces = await _namespaces(info)
        access_tags = UNSET
        if input.access_tags is not UNSET:
            requested_access_tags = (
                _access_tags_from_input(input.access_tags)
                if input.access_tags is not None
                else None
            )
            access_tags = await _modify_access_tags(
                info, normalize_access_tags(current.access_tags), requested_access_tags
            )
        predicate = (
            STORE_UNSET
            if input.predicate is UNSET
            else expand_term(input.predicate, namespaces)
        )
        record = await _store(info).update_link(
            str(id),
            predicate=predicate,
            access_tags=STORE_UNSET if access_tags is UNSET else access_tags,
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
        _cache(info).invalidate_namespaces()
        logger.info("Upserted namespace prefix=%r uri=%r", prefix, uri)
        return Namespace(prefix=prefix, uri=uri)

    @strawberry.mutation(
        description="Delete a registered namespace. Returns true if found."
    )
    async def delete_namespace(self, info: Info, prefix: str) -> bool:
        _assert_authn_scope(info, "write:metadata")
        deleted = await _store(info).delete_namespace(prefix)
        if deleted:
            _cache(info).invalidate_namespaces()
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
