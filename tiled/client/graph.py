"""
Client-side helpers for the "graph of links" feature.

The server exposes a GraphQL API (at `/api/graphql`) for a small graph of
**entities** (named, typed nodes, optionally bound to a catalog node) connected
by directed, predicate-labeled **links**. This module wraps that API so it can
be driven from ordinary Python without hand-writing GraphQL.

Two ways in:

* Bind an entity to a dataset you already have a client for::

      e1 = array_client.bind_entity()            # kind/name default from the node
      e2 = table_client.bind_entity(name="cal", kind="reference")

* Create a free-standing (external) entity that has no catalog node behind it::

      sample = make_entity(client, name="LaB6", kind="sample",
                           uri="http://www.wikidata.org/entity/Q...")

Then connect any two entity handles (the direction is `subject -predicate->
object`)::

      make_link(subject=e2, object=e1, predicate="prov:wasDerivedFrom")

`kind` is a free-form label for an entity (the server's `kind` field; not
tied to a namespace). Link `predicate` values and property keys may be CURIEs
(e.g. `prov:wasDerivedFrom`) once the prefix has been registered with
:func:`register_namespace`.
"""

from __future__ import annotations

from typing import Any, Optional

from .utils import handle_error, retry_context

__all__ = [
    "GraphClient",
    "EntityHandle",
    "LinkHandle",
    "GraphError",
    "EntityExistsError",
    "make_entity",
    "make_link",
    "register_namespace",
]

# GraphQL documents. Strawberry camelCases field/argument names, so the
# server's `kind`/`node_path_parts` are `kind`/`nodePathParts` here.
_ENTITY_FIELDS = "id isNodeBound kind name uri properties createdAt"
_LINK_FIELDS = "id subjectId predicate objectId properties"

_CREATE_ENTITY = f"""
mutation($input: CreateEntityInput!) {{
  createEntity(input: $input) {{ {_ENTITY_FIELDS} }}
}}
"""
_CREATE_LINK = f"""
mutation($input: CreateLinkInput!) {{
  createLink(input: $input) {{ {_LINK_FIELDS} }}
}}
"""
_ENTITIES = f"""
query($kind: String, $nodePathParts: [String!], $name: String,
      $limit: Int!, $offset: Int!) {{
  entities(kind: $kind, nodePathParts: $nodePathParts, name: $name,
           limit: $limit, offset: $offset) {{ {_ENTITY_FIELDS} }}
}}
"""
_ENTITY = f"query($id: ID!) {{ entity(id: $id) {{ {_ENTITY_FIELDS} }} }}"
_DELETE_ENTITY = "mutation($id: ID!) { deleteEntity(id: $id) }"
_DELETE_LINK = "mutation($id: ID!) { deleteLink(id: $id) }"
_NAMESPACES = "{ namespaces { prefix uri } }"
_UPSERT_NAMESPACE = """
mutation($prefix: String!, $uri: String!) {
  upsertNamespace(prefix: $prefix, uri: $uri) { prefix }
}
"""


class GraphError(RuntimeError):
    "The GraphQL endpoint returned one or more errors."


class EntityExistsError(ValueError):
    "An entity with the same (node, kind, name) already exists."


class GraphClient:
    """Access to a Tiled server's graph-of-links GraphQL API.

    Construct one from any Tiled client (it borrows the client's authenticated
    HTTP connection) or directly from a :class:`~tiled.client.context.Context`.
    """

    def __init__(self, context):
        self._context = context
        # The GraphQL endpoint lives at `/api/graphql`, a sibling of the REST
        # API at `/api/v1/`. Derive its absolute URL from the client's
        # api_uri so host/port and any server root_path prefix are handled for
        # us (the http_client base_url is empty for `from_uri` clients).
        api_uri = str(context.api_uri).rstrip("/")  # e.g. .../api/v1
        self._url = api_uri.removesuffix("/v1") + "/graphql"

    def __repr__(self):
        return f"<GraphClient {self._url!r}>"

    def _execute(self, query: str, variables: Optional[dict] = None) -> dict:
        for attempt in retry_context():
            with attempt:
                response = handle_error(
                    self._context.http_client.post(
                        self._url,
                        json={"query": query, "variables": variables or {}},
                    )
                )
        payload = response.json()
        if payload.get("errors"):
            errors = payload["errors"]
            # The server tags a unique-constraint violation with this code so
            # the client can surface a specific, catchable exception rather
            # than a generic GraphError.
            for error in errors:
                if (error.get("extensions") or {}).get("code") == "ENTITY_EXISTS":
                    raise EntityExistsError(error.get("message", "Entity already exists."))
            raise GraphError(errors)
        return payload["data"]

    @property
    def namespaces(self) -> dict[str, str]:
        "Registered CURIE prefix -> URI mappings (global to the server)."
        rows = self._execute(_NAMESPACES)["namespaces"]
        return {row["prefix"]: row["uri"] for row in rows}

    def register_namespace(self, prefix: str, uri: str) -> None:
        """Register (or update) a CURIE prefix

        Once registered, the namespace can be used in predicates and
        property keys. This is a global, idempotent upsert: once registered,
        the prefix is available to every user of the server.
        """
        self._execute(_UPSERT_NAMESPACE, {"prefix": prefix, "uri": uri})

    def create_entity(
        self,
        *,
        kind: str,
        name: str,
        node_path_parts: Optional[list[str]] = None,
        uri: Optional[str] = None,
        properties: Optional[dict[str, Any]] = None,
    ) -> "EntityHandle":
        "Create a graph entity. Prefer `node.bind_entity` / :func:`make_entity`."
        input: dict[str, Any] = {
            "kind": kind,
            "name": name,
            "uri": uri,
            "properties": properties or {},
        }
        if node_path_parts is not None:
            input["nodePathParts"] = node_path_parts
        data = self._execute(_CREATE_ENTITY, {"input": input})
        return EntityHandle._from_json(self, data["createEntity"])

    def get_entity(self, id: str) -> Optional["EntityHandle"]:
        "Fetch a single entity by id; returns None if it does not exist / is hidden."
        record = self._execute(_ENTITY, {"id": id})["entity"]
        return EntityHandle._from_json(self, record) if record else None

    def find_entities(
        self,
        *,
        kind: Optional[str] = None,
        name: Optional[str] = None,
        node_path_parts: Optional[list[str]] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list["EntityHandle"]:
        "List entities, optionally filtered by kind, name, and/or bound node."
        records = self._execute(
            _ENTITIES,
            {
                "kind": kind,
                "name": name,
                "nodePathParts": node_path_parts,
                "limit": limit,
                "offset": offset,
            },
        )["entities"]
        return [EntityHandle._from_json(self, r) for r in records]

    def delete_entity(self, id: str) -> bool:
        "Delete an entity by id, along with all links attached to it."
        return bool(self._execute(_DELETE_ENTITY, {"id": id})["deleteEntity"])

    def create_link(
        self,
        *,
        subject_id: str,
        predicate: str,
        object_id: str,
        properties: Optional[dict[str, Any]] = None,
    ) -> "LinkHandle":
        "Create a `subject -predicate-> object` link. Prefer :func:`make_link`."
        data = self._execute(
            _CREATE_LINK,
            {
                "input": {
                    "subjectId": subject_id,
                    "predicate": predicate,
                    "objectId": object_id,
                    "properties": properties or {},
                }
            },
        )
        return LinkHandle._from_json(self, data["createLink"])

    def delete_link(self, id: str) -> bool:
        "Delete a link by id."
        return bool(self._execute(_DELETE_LINK, {"id": id})["deleteLink"])


class EntityHandle:
    """A client-side handle to an entity in the graph.
    
    Carries the connection it was created through, so it can be passed
    straight to :func:`make_link`.
    """

    def __init__(
        self,
        graph: GraphClient,
        *,
        id: str,
        kind: str,
        name: str,
        is_node_bound: bool = False,
        uri: Optional[str] = None,
        properties: Optional[dict] = None,
    ):
        self._graph = graph
        self.id = id
        self.kind = kind
        self.name = name
        # Whether this entity is bound to a catalog node. The node's internal
        # id is a server-side detail and is deliberately not exposed here.
        self.is_node_bound = is_node_bound
        self.uri = uri
        self.properties = properties or {}

    @classmethod
    def _from_json(cls, graph: GraphClient, record: dict) -> "EntityHandle":
        return cls(
            graph,
            id=record["id"],
            kind=record["kind"],
            name=record["name"],
            is_node_bound=record.get("isNodeBound", False),
            uri=record.get("uri"),
            properties=record.get("properties") or {},
        )

    def __repr__(self):
        return f"<EntityHandle kind={self.kind!r} name={self.name!r} id={self.id!r}>"

    def delete(self) -> bool:
        "Delete this entity and all links attached to it."
        return self._graph.delete_entity(self.id)


class LinkHandle:
    "A handle to a directed link between two entities."

    def __init__(
        self,
        graph: GraphClient,
        *,
        id: str,
        subject_id: str,
        predicate: str,
        object_id: str,
        properties: Optional[dict] = None,
    ):
        self._graph = graph
        self.id = id
        self.subject_id = subject_id
        self.predicate = predicate
        self.object_id = object_id
        self.properties = properties or {}

    @classmethod
    def _from_json(cls, graph: GraphClient, record: dict) -> "LinkHandle":
        return cls(
            graph,
            id=record["id"],
            subject_id=record["subjectId"],
            predicate=record["predicate"],
            object_id=record["objectId"],
            properties=record.get("properties") or {},
        )

    def __repr__(self):
        return (
            f"<LinkHandle {self.subject_id!r} -[{self.predicate}]-> "
            f"{self.object_id!r}>"
        )

    def delete(self) -> bool:
        "Delete this link."
        return self._graph.delete_link(self.id)


def _graph_for(obj) -> GraphClient:
    "Coerce a Tiled client, a Context, or a GraphClient into a GraphClient."
    if isinstance(obj, GraphClient):
        return obj
    context = getattr(obj, "context", obj)
    return GraphClient(context)


def make_entity(
    client,
    *,
    kind: str,
    name: str,
    uri: Optional[str] = None,
    properties: Optional[dict[str, Any]] = None,
) -> EntityHandle:
    """
    Create a free-standing (external) entity with no catalog node behind it,
    borrowing `client`'s connection. Use this for things that live outside
    this Tiled server, e.g. a sample, an instrument, or a processing script.
    """
    return _graph_for(client).create_entity(
        kind=kind, name=name, node_path_parts=None, uri=uri, properties=properties
    )


def make_link(
    *,
    subject: EntityHandle,
    object: EntityHandle,
    predicate: str,
    properties: Optional[dict[str, Any]] = None,
) -> LinkHandle:
    """Create a directed link between two entityhandles
    
    The link takes the form of `subject -predicate-> object`, where `subject`
    and `object` are keyword-only so the direction is unambiguous
    (e.g. `subtracted` was derived from `measured`:
    `make_link(subject=subtracted, object=measured,
    predicate="prov:wasDerivedFrom")`).
    """
    return subject._graph.create_link(
        subject_id=subject.id,
        predicate=predicate,
        object_id=object.id,
        properties=properties,
    )


def register_namespace(client, prefix: str, uri: str) -> None:
    """Register (or update) a CURIE prefix -> URI mapping
    
    This borrows `client`'s connection. The changes are global and idempotent;
    see :meth:`GraphClient.register_namespace`.
    """
    _graph_for(client).register_namespace(prefix, uri)
