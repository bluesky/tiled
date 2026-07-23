"""
Links router for the Tiled service.

Provides a GraphQL interface for the entity/link graph under /api/v1/links.
The store lifecycle is owned by the router: startup/shutdown handlers are
registered automatically when the router is included in a FastAPI app.

Database migrations are NOT run here — they are the responsibility of the
caller (app startup) following the same pattern as the authn and catalog
databases. The links tables are managed by catalog migrations, so use
`tiled catalog init` / `upgrade-database` for the target database, or let
the server auto-initialize when database_init_if_not_exists is set.
"""

from __future__ import annotations

import logging
from typing import Callable, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from strawberry.fastapi import GraphQLRouter

from ..access_control.access_policies import NO_ACCESS
from ..server.authentication import (
    get_current_access_tags,
    get_current_principal,
    get_current_scopes,
)
from ..server.schemas import Principal
from ..server.settings import DatabaseSettings
from ..type_aliases import AccessTags, Scopes
from .curie import (
    collect_used_prefixes_from_term,
    collect_used_prefixes_from_value,
    compact_term,
    compact_value,
    expand_term,
    expand_value,
)
from .schema import _matches_access_blob_filter, _PolicyNode, schema
from .store import EntityRecord, GraphSQLAlchemyStore, LinkRecord

logger = logging.getLogger(__name__)


def _extract_context_namespaces(context) -> dict[str, str]:
    if not isinstance(context, dict):
        return {}
    out: dict[str, str] = {}
    for key, value in context.items():
        if key.startswith("@"):
            continue
        if key in {"subject", "object"}:
            continue
        if isinstance(value, str):
            out[key] = value
    return out


def _assert_scope(authn_scopes: Scopes, scope: str) -> None:
    if scope not in authn_scopes:
        raise HTTPException(status_code=403, detail="Not permitted")


async def _is_allowed(
    *,
    access_blob: Optional[dict],
    policy,
    principal: Optional[Principal],
    authn_access_tags: Optional[AccessTags],
    authn_scopes: Scopes,
    scope: str,
) -> bool:
    if scope not in authn_scopes:
        return False
    if policy is None or not hasattr(policy, "allowed_scopes"):
        return True
    allowed = await policy.allowed_scopes(
        _PolicyNode(access_blob),
        principal,
        authn_access_tags,
        authn_scopes,
    )
    return scope in allowed


async def _apply_filters(
    *,
    records: list,
    policy,
    principal: Optional[Principal],
    authn_access_tags: Optional[AccessTags],
    authn_scopes: Scopes,
    scope: str,
) -> list:
    if policy is None or not hasattr(policy, "filters"):
        return records
    queries = await policy.filters(
        _PolicyNode({}),
        principal,
        authn_access_tags,
        authn_scopes,
        {scope},
    )
    if queries is NO_ACCESS:
        return []
    if not queries:
        return records

    filtered = records
    for query in queries:
        if hasattr(query, "tags") and hasattr(query, "user_id"):
            filtered = [
                record
                for record in filtered
                if _matches_access_blob_filter(record.access_blob or {}, query)
            ]
        else:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Unsupported access-policy filter for JSON-LD export: "
                    f"{type(query).__name__}"
                ),
            )
    return filtered


async def _list_all_entities(store: GraphSQLAlchemyStore) -> list[EntityRecord]:
    out = []
    offset = 0
    page_size = 500
    while True:
        page = await store.list_entities(limit=page_size, offset=offset)
        if not page:
            break
        out.extend(page)
        if len(page) < page_size:
            break
        offset += page_size
    return out


async def _list_all_links(store: GraphSQLAlchemyStore) -> list[LinkRecord]:
    out = []
    offset = 0
    page_size = 500
    while True:
        page = await store.find_links(limit=page_size, offset=offset)
        if not page:
            break
        out.extend(page)
        if len(page) < page_size:
            break
        offset += page_size
    return out


def create_router(get_database_settings: Callable[[], DatabaseSettings]) -> APIRouter:
    store: list[GraphSQLAlchemyStore] = []  # mutable cell — populated on startup

    async def startup() -> None:
        db_settings = get_database_settings()
        logger.info("Initializing links store with shared DB pool: %s", db_settings.uri)
        store.append(await GraphSQLAlchemyStore.from_database_settings(db_settings))

    async def shutdown() -> None:
        if store:
            await store[0].close()
            logger.info("Links store closed")

    async def get_context(
        request: Request,
        principal=Depends(get_current_principal),
        authn_access_tags=Depends(get_current_access_tags),
        authn_scopes=Depends(get_current_scopes),
    ) -> dict:
        return {
            "store": store[0],
            "principal": principal,
            "authn_access_tags": authn_access_tags,
            "authn_scopes": authn_scopes,
            "access_policy": getattr(request.app.state, "access_policy", None),
        }

    graphql_router = GraphQLRouter(
        schema,
        context_getter=get_context,
        graphql_ide="graphiql",
    )

    router = APIRouter(on_startup=[startup], on_shutdown=[shutdown])
    router.include_router(graphql_router, prefix="/api/graphql")

    @router.get("/api/v1/graph/jsonld")
    async def export_jsonld(
        request: Request,
        principal=Depends(get_current_principal),
        authn_access_tags=Depends(get_current_access_tags),
        authn_scopes=Depends(get_current_scopes),
    ):
        entities = await _list_all_entities(store[0])
        links = await _list_all_links(store[0])
        policy = getattr(request.app.state, "access_policy", None)

        entities = await _apply_filters(
            records=entities,
            policy=policy,
            principal=principal,
            authn_access_tags=authn_access_tags,
            authn_scopes=authn_scopes,
            scope="read:metadata",
        )
        links = await _apply_filters(
            records=links,
            policy=policy,
            principal=principal,
            authn_access_tags=authn_access_tags,
            authn_scopes=authn_scopes,
            scope="read:metadata",
        )

        allowed_entities = [
            entity
            for entity in entities
            if await _is_allowed(
                access_blob=entity.access_blob,
                policy=policy,
                principal=principal,
                authn_access_tags=authn_access_tags,
                authn_scopes=authn_scopes,
                scope="read:metadata",
            )
        ]
        visible_entity_ids = {entity.id for entity in allowed_entities}

        allowed_links = [
            link
            for link in links
            if (
                link.subject_id in visible_entity_ids
                and link.object_id in visible_entity_ids
                and await _is_allowed(
                    access_blob=link.access_blob,
                    policy=policy,
                    principal=principal,
                    authn_access_tags=authn_access_tags,
                    authn_scopes=authn_scopes,
                    scope="read:metadata",
                )
            )
        ]

        namespaces = await store[0].list_namespaces()
        used_prefixes: set[str] = set()

        graph_items = []
        for entity in allowed_entities:
            compact_properties = compact_value(entity.properties, namespaces)
            used_prefixes.update(
                collect_used_prefixes_from_value(entity.properties, namespaces)
            )
            graph_items.append(
                {
                    "@id": f"urn:entity:{entity.id}",
                    "@type": "Entity",
                    "entityType": entity.entity_type,
                    "name": entity.name,
                    "nodeId": entity.node_id,
                    "uri": entity.uri,
                    "properties": compact_properties,
                    "accessBlob": entity.access_blob,
                }
            )

        for link in allowed_links:
            compact_predicate = compact_term(link.predicate, namespaces)
            compact_properties = compact_value(link.properties, namespaces)
            used_prefixes.update(
                collect_used_prefixes_from_term(link.predicate, namespaces)
            )
            used_prefixes.update(
                collect_used_prefixes_from_value(link.properties, namespaces)
            )
            graph_items.append(
                {
                    "@id": f"urn:link:{link.id}",
                    "@type": "Link",
                    "subject": f"urn:entity:{link.subject_id}",
                    "predicate": compact_predicate,
                    "object": f"urn:entity:{link.object_id}",
                    "properties": compact_properties,
                    "accessBlob": link.access_blob,
                }
            )

        export_namespaces = {
            prefix: namespaces[prefix]
            for prefix in sorted(used_prefixes)
            if prefix in namespaces
        }

        payload = {
            "@context": {
                "@vocab": "https://blueskyproject.io/tiled/graph#",
                **export_namespaces,
                "subject": {"@type": "@id"},
                "object": {"@type": "@id"},
            },
            "@graph": graph_items,
        }
        return JSONResponse(payload, media_type="application/ld+json")

    @router.post("/api/v1/graph/jsonld")
    async def import_jsonld(
        request: Request,
        principal=Depends(get_current_principal),
        authn_access_tags=Depends(get_current_access_tags),
        authn_scopes=Depends(get_current_scopes),
    ):
        _assert_scope(authn_scopes, "write:metadata")
        policy = getattr(request.app.state, "access_policy", None)
        data = await request.json()
        if not isinstance(data, dict) or not isinstance(data.get("@graph"), list):
            raise HTTPException(
                status_code=400,
                detail="JSON-LD body must be an object containing '@graph' list",
            )

        context_namespaces = _extract_context_namespaces(data.get("@context"))
        for prefix, uri in context_namespaces.items():
            await store[0].upsert_namespace(prefix, uri)

        graph_items = data["@graph"]
        entity_id_map: dict[str, str] = {}
        created_entities = 0
        created_links = 0

        known_entity_fields = {
            "@id",
            "@type",
            "entityType",
            "name",
            "nodeId",
            "uri",
            "properties",
            "accessBlob",
        }
        known_link_fields = {
            "@id",
            "@type",
            "subject",
            "predicate",
            "object",
            "properties",
            "accessBlob",
        }

        for item in graph_items:
            if not isinstance(item, dict):
                raise HTTPException(
                    status_code=400, detail="Items in '@graph' must be objects"
                )
            if item.get("@type") != "Entity":
                continue

            access_blob = item.get("accessBlob")
            if access_blob is None:
                access_blob = {}
            if policy is not None and hasattr(policy, "init_node"):
                try:
                    _, access_blob = await policy.init_node(
                        principal,
                        authn_access_tags,
                        authn_scopes,
                        access_blob=access_blob,
                    )
                except ValueError as exc:
                    raise HTTPException(
                        status_code=403,
                        detail=f"Access policy rejects access blob: {exc}",
                    ) from exc

            raw_properties = item.get("properties")
            if raw_properties is None:
                raw_properties = {}
            if not isinstance(raw_properties, dict):
                raise HTTPException(
                    status_code=400,
                    detail="Entity 'properties' must be an object",
                )

            extra_fields = {
                key: value
                for key, value in item.items()
                if key not in known_entity_fields
            }
            merged_properties = {
                **raw_properties,
                **extra_fields,
            }

            record = await store[0].create_entity(
                entity_type=item.get("entityType", "entity"),
                name=item.get("name") or "",
                node_id=item.get("nodeId"),
                uri=item.get("uri"),
                properties=expand_value(merged_properties, context_namespaces),
                access_blob=access_blob,
            )
            imported_id = item.get("@id")
            if isinstance(imported_id, str):
                entity_id_map[imported_id] = record.id
            created_entities += 1

        for item in graph_items:
            if not isinstance(item, dict):
                continue
            if item.get("@type") != "Link":
                continue

            subject_ref = item.get("subject")
            object_ref = item.get("object")
            subject_id = entity_id_map.get(subject_ref, subject_ref)
            object_id = entity_id_map.get(object_ref, object_ref)
            if not isinstance(subject_id, str) or not isinstance(object_id, str):
                raise HTTPException(
                    status_code=400,
                    detail="Link 'subject' and 'object' must be string identifiers",
                )
            if subject_id.startswith("urn:entity:"):
                subject_id = subject_id.removeprefix("urn:entity:")
            if object_id.startswith("urn:entity:"):
                object_id = object_id.removeprefix("urn:entity:")

            subject = await store[0].get_entity(subject_id)
            object_ = await store[0].get_entity(object_id)
            if subject is None or object_ is None:
                raise HTTPException(
                    status_code=400,
                    detail="Link references missing subject/object entity",
                )

            if not await _is_allowed(
                access_blob=subject.access_blob,
                policy=policy,
                principal=principal,
                authn_access_tags=authn_access_tags,
                authn_scopes=authn_scopes,
                scope="write:metadata",
            ):
                raise HTTPException(status_code=403, detail="Not permitted")
            if not await _is_allowed(
                access_blob=object_.access_blob,
                policy=policy,
                principal=principal,
                authn_access_tags=authn_access_tags,
                authn_scopes=authn_scopes,
                scope="write:metadata",
            ):
                raise HTTPException(status_code=403, detail="Not permitted")

            access_blob = item.get("accessBlob")
            if access_blob is None:
                access_blob = {}
            if policy is not None and hasattr(policy, "init_node"):
                try:
                    _, access_blob = await policy.init_node(
                        principal,
                        authn_access_tags,
                        authn_scopes,
                        access_blob=access_blob,
                    )
                except ValueError as exc:
                    raise HTTPException(
                        status_code=403,
                        detail=f"Access policy rejects access blob: {exc}",
                    ) from exc

            raw_properties = item.get("properties")
            if raw_properties is None:
                raw_properties = {}
            if not isinstance(raw_properties, dict):
                raise HTTPException(
                    status_code=400,
                    detail="Link 'properties' must be an object",
                )
            extra_fields = {
                key: value
                for key, value in item.items()
                if key not in known_link_fields
            }
            merged_properties = {
                **raw_properties,
                **extra_fields,
            }

            raw_predicate = item.get("predicate") or "relatedTo"
            predicate = (
                expand_term(raw_predicate, context_namespaces)
                if isinstance(raw_predicate, str)
                else "relatedTo"
            )

            await store[0].create_link(
                subject_id=subject_id,
                predicate=predicate,
                object_id=object_id,
                properties=expand_value(merged_properties, context_namespaces),
                access_blob=access_blob,
            )
            created_links += 1

        return {
            "created_entities": created_entities,
            "created_links": created_links,
        }

    return router
