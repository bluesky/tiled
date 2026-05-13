"""
Strawberry GraphQL schema for splash-links.

Graph model:
  - Entity  — a named node with a type and arbitrary JSON properties
  - Link     — a directed, predicate-labeled edge between two entities

Query highlights:
    - entity / entities — fetch nodes
    - link / links      — fetch edges, filterable by subject, predicate, object
    - Entity.outgoing_links / incoming_links — graph traversal from a node

Mutations:
    - createEntity / createLink
    - deleteEntity (cascades to attached links) / deleteLink
"""

from __future__ import annotations

import logging
from typing import Optional

import strawberry
from strawberry.scalars import JSON as StrawberryJSON
from strawberry.types import Info

from .store import EntityRecord, LinkRecord, Store

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# JSON scalar — pass arbitrary dicts / lists / primitives through GraphQL
# ---------------------------------------------------------------------------

JSON = StrawberryJSON


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _store(info: Info) -> Store:
    return info.context["store"]


# ---------------------------------------------------------------------------
# Output types
# ---------------------------------------------------------------------------


@strawberry.type
class Entity:
    id: strawberry.ID
    entity_type: str
    name: str
    uri: Optional[str]
    properties: Optional[JSON]  # type: ignore[valid-type]
    created_at: str

    @strawberry.field(description="Links where this entity is the subject.")
    def outgoing_links(
        self,
        info: Info,
        predicate: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list["Link"]:
        records = _store(info).find_links(
            subject_id=str(self.id), predicate=predicate, limit=limit, offset=offset
        )
        return [_link_from_record(r) for r in records]

    @strawberry.field(description="Links where this entity is the object.")
    def incoming_links(
        self,
        info: Info,
        predicate: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list["Link"]:
        records = _store(info).find_links(
            object_id=str(self.id), predicate=predicate, limit=limit, offset=offset
        )
        return [_link_from_record(r) for r in records]


@strawberry.type
class Link:
    id: strawberry.ID
    subject_id: strawberry.ID
    predicate: str
    object_id: strawberry.ID
    properties: Optional[JSON]  # type: ignore[valid-type]
    created_at: str

    @strawberry.field
    def subject(self, info: Info) -> Optional[Entity]:
        record = _store(info).get_entity(str(self.subject_id))
        return _entity_from_record(record) if record else None

    @strawberry.field
    def object(self, info: Info) -> Optional[Entity]:
        record = _store(info).get_entity(str(self.object_id))
        return _entity_from_record(record) if record else None


# ---------------------------------------------------------------------------
# Record -> GQL type converters
# ---------------------------------------------------------------------------


def _entity_from_record(r: EntityRecord) -> Entity:
    return Entity(
        id=strawberry.ID(r.id),
        entity_type=r.entity_type,
        name=r.name,
        uri=r.uri,
        properties=r.properties if r.properties else None,
        created_at=r.created_at.isoformat(),
    )


def _link_from_record(r: LinkRecord) -> Link:
    return Link(
        id=strawberry.ID(r.id),
        subject_id=strawberry.ID(r.subject_id),
        predicate=r.predicate,
        object_id=strawberry.ID(r.object_id),
        properties=r.properties if r.properties else None,
        created_at=r.created_at.isoformat(),
    )


# ---------------------------------------------------------------------------
# Input types
# ---------------------------------------------------------------------------


@strawberry.input
class UpdateEntityInput:
    name: Optional[str] = None
    uri: Optional[str] = None
    entity_type: Optional[str] = None


@strawberry.input
class UpdateLinkInput:
    predicate: str


@strawberry.input
class CreateEntityInput:
    entity_type: str
    name: str
    uri: Optional[str] = None
    properties: Optional[JSON] = None  # type: ignore[valid-type]


@strawberry.input
class CreateLinkInput:
    subject_id: strawberry.ID
    predicate: str
    object_id: strawberry.ID
    properties: Optional[JSON] = None  # type: ignore[valid-type]


# ---------------------------------------------------------------------------
# Query / Mutation
# ---------------------------------------------------------------------------


@strawberry.type
class Query:
    @strawberry.field
    def entity(self, info: Info, id: strawberry.ID) -> Optional[Entity]:
        record = _store(info).get_entity(str(id))
        return _entity_from_record(record) if record else None

    @strawberry.field
    def entities(
        self,
        info: Info,
        entity_type: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Entity]:
        records = _store(info).list_entities(
            entity_type=entity_type, limit=limit, offset=offset
        )
        return [_entity_from_record(r) for r in records]

    @strawberry.field
    def link(self, info: Info, id: strawberry.ID) -> Optional[Link]:
        record = _store(info).get_link(str(id))
        return _link_from_record(record) if record else None

    @strawberry.field(
        description="Find links, optionally filtered by subject, predicate, and/or object."
    )
    def links(
        self,
        info: Info,
        subject_id: Optional[strawberry.ID] = None,
        predicate: Optional[str] = None,
        object_id: Optional[strawberry.ID] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Link]:
        records = _store(info).find_links(
            subject_id=str(subject_id) if subject_id else None,
            predicate=predicate,
            object_id=str(object_id) if object_id else None,
            limit=limit,
            offset=offset,
        )
        return [_link_from_record(r) for r in records]


@strawberry.type
class Mutation:
    @strawberry.mutation
    def create_entity(self, info: Info, input: CreateEntityInput) -> Entity:
        record = _store(info).create_entity(
            entity_type=input.entity_type,
            name=input.name,
            uri=input.uri,
            properties=input.properties,
        )
        logger.info(
            "Created entity type=%r name=%r id=%s",
            record.entity_type,
            record.name,
            record.id,
        )
        return _entity_from_record(record)

    @strawberry.mutation
    def create_link(self, info: Info, input: CreateLinkInput) -> Link:
        record = _store(info).create_link(
            subject_id=str(input.subject_id),
            predicate=input.predicate,
            object_id=str(input.object_id),
            properties=input.properties,
        )
        logger.info(
            "Created link %s -[%s]-> %s id=%s",
            record.subject_id[:8],
            record.predicate,
            record.object_id[:8],
            record.id,
        )
        return _link_from_record(record)

    @strawberry.mutation(
        description="Delete an entity and all its attached links. Returns true if found."
    )
    def delete_entity(self, info: Info, id: strawberry.ID) -> bool:
        deleted = _store(info).delete_entity(str(id))
        if deleted:
            logger.info("Deleted entity id=%s", id)
        return deleted

    @strawberry.mutation(description="Update an entity's name, uri, or entity_type.")
    def update_entity(
        self, info: Info, id: strawberry.ID, input: UpdateEntityInput
    ) -> Optional[Entity]:
        record = _store(info).update_entity(
            str(id),
            name=input.name,
            uri=input.uri,
            entity_type=input.entity_type,
        )
        if record:
            logger.info("Updated entity id=%s", id)
        return _entity_from_record(record) if record else None

    @strawberry.mutation(description="Delete a single link. Returns true if found.")
    def delete_link(self, info: Info, id: strawberry.ID) -> bool:
        deleted = _store(info).delete_link(str(id))
        if deleted:
            logger.info("Deleted link id=%s", id)
        return deleted

    @strawberry.mutation(description="Update a link's predicate.")
    def update_link(
        self, info: Info, id: strawberry.ID, input: UpdateLinkInput
    ) -> Optional[Link]:
        record = _store(info).update_link(str(id), predicate=input.predicate)
        if record:
            logger.info("Updated link id=%s predicate=%r", id, input.predicate)
        return _link_from_record(record) if record else None


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

schema = strawberry.Schema(query=Query, mutation=Mutation)
