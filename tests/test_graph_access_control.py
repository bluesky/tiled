import pytest
from sqlalchemy import insert as sa_insert
from sqlalchemy.exc import IntegrityError
from starlette.testclient import TestClient

from tiled.catalog import in_memory
from tiled.catalog.core import initialize_database
from tiled.config import Database
from tiled.graph.schema import schema
from tiled.graph.store import EntityConflictError, GraphSQLAlchemyStore, _nodes
from tiled.queries import AccessBlobFilter
from tiled.server.app import build_app
from tiled.server.authentication import (
    get_current_access_tags,
    get_current_principal,
    get_current_scopes,
)
from tiled.server.connection_pool import (
    close_database_connection_pool,
    get_database_engine,
)
from tiled.server.settings import DatabaseSettings

CREATE_ENTITY_MUTATION = """
mutation($input: CreateEntityInput!) {
    createEntity(input: $input) { id name }
}
"""

READ_ENTITY_QUERY = "query($id: ID!) { entity(id: $id) { id name } }"

CREATE_LINK_MUTATION = """
mutation($input: CreateLinkInput!) {
    createLink(input: $input) { id predicate }
}
"""


class FakeTagPolicy:
    """Policy stub mirroring the node access_blob lifecycle and scope checks."""

    def __init__(self, user_tags):
        self.user_tags = user_tags

    async def init_node(
        self,
        principal,
        authn_access_tags,
        authn_scopes,
        access_blob=None,
    ):
        if access_blob is None:
            return (True, {"user": principal})
        if "tags" in access_blob:
            return (False, access_blob)
        raise ValueError(
            "access_blob must be either null or {'tags': [...]} for this test"
        )

    async def modify_node(
        self,
        node,
        principal,
        authn_access_tags,
        authn_scopes,
        access_blob,
    ):
        if "tags" in access_blob:
            return (False, access_blob)
        raise ValueError("access_blob updates must use {'tags': [...]} for this test")

    async def allowed_scopes(
        self,
        node,
        principal,
        authn_access_tags,
        authn_scopes,
    ):
        access_blob = getattr(node, "access_blob", {}) or {}
        if access_blob.get("user") == principal:
            return set(authn_scopes)
        tags = set(access_blob.get("tags", []))
        if tags.intersection(self.user_tags.get(principal, set())):
            return set(authn_scopes)
        return set()

    async def filters(
        self,
        node,
        principal,
        authn_access_tags,
        authn_scopes,
        scopes,
    ):
        return []


class FilterPolicy(FakeTagPolicy):
    def __init__(self, user_tags):
        super().__init__(user_tags)
        self.filter_calls = 0

    async def filters(
        self,
        node,
        principal,
        authn_access_tags,
        authn_scopes,
        scopes,
    ):
        self.filter_calls += 1
        return [AccessBlobFilter(user_id=None, tags=["team"])]


@pytest.fixture
async def store():
    database_settings = DatabaseSettings(uri="sqlite:///:memory:")
    # tiled.graph.orm's tables live on the catalog's Base.metadata (already
    # imported transitively via tiled.graph.store), so a single
    # initialize_database() call provisions nodes, entities, links, and
    # namespaces together -- the store itself no longer creates tables.
    engine = get_database_engine(database_settings)
    await initialize_database(engine)
    s = await GraphSQLAlchemyStore.from_database_settings(database_settings)
    yield s
    # Tear down the shared pool entry (rather than just `s.close()`, which is
    # a no-op here) so each test gets an isolated in-memory database instead
    # of silently reusing state left behind by the previous test.
    await close_database_connection_pool(database_settings)


@pytest.fixture
def policy():
    return FakeTagPolicy(
        {
            "alice": {"alice_tag", "team"},
            "bob": {"team"},
        }
    )


@pytest.fixture
def filter_policy():
    return FilterPolicy(
        {
            "alice": {"alice_tag", "team"},
            "bob": {"team"},
        }
    )


def _context(store, policy, principal, scopes):
    return {
        "store": store,
        "principal": principal,
        "authn_access_tags": None,
        "authn_scopes": set(scopes),
        "access_policy": policy,
    }


async def _execute(query, context, variables=None):
    result = await schema.execute(
        query,
        variable_values=variables or {},
        context_value=context,
    )
    return result


async def _insert_node(store, node_id, access_blob, key="node", parent=None):
    """Insert a synthetic catalog node row for entity/node delegation tests."""
    async with store._engine.begin() as conn:
        await conn.execute(
            sa_insert(_nodes).values(
                id=node_id,
                parent=parent,
                key=key,
                structure_family="container",
                metadata={},
                specs=[],
                access_blob=access_blob,
            )
        )


@pytest.mark.asyncio
async def test_entity_create_defaults_to_user_access_blob_and_read_visibility(
    store, policy
):
    """Create without tags, verify user-owned blob and read visibility rules."""

    alice_ctx = _context(store, policy, "alice", {"read:metadata", "write:metadata"})
    result = await _execute(
        CREATE_ENTITY_MUTATION,
        alice_ctx,
        {"input": {"kind": "sample", "name": "E1", "properties": {}}},
    )
    assert result.errors is None
    entity_id = result.data["createEntity"]["id"]

    record = await store.get_entity(entity_id)
    assert record.access_blob == {"user": "alice"}

    alice_read = await _execute(READ_ENTITY_QUERY, alice_ctx, {"id": entity_id})
    assert alice_read.errors is None
    assert alice_read.data["entity"]["id"] == entity_id

    bob_ctx = _context(store, policy, "bob", {"read:metadata", "write:metadata"})
    bob_read = await _execute(READ_ENTITY_QUERY, bob_ctx, {"id": entity_id})
    assert bob_read.errors is None
    assert bob_read.data["entity"] is None


@pytest.mark.asyncio
async def test_entity_can_be_tagged_and_shared_for_reads(store, policy):
    """Create with a shared tag and verify another user can read it."""

    alice_ctx = _context(store, policy, "alice", {"read:metadata", "write:metadata"})
    result = await _execute(
        CREATE_ENTITY_MUTATION,
        alice_ctx,
        {
            "input": {
                "kind": "sample",
                "name": "Etag",
                "properties": {},
                "accessBlob": {"tags": ["team"]},
            }
        },
    )
    assert result.errors is None
    entity_id = result.data["createEntity"]["id"]

    bob_ctx = _context(store, policy, "bob", {"read:metadata"})
    bob_read = await _execute(READ_ENTITY_QUERY, bob_ctx, {"id": entity_id})
    assert bob_read.errors is None
    assert bob_read.data["entity"]["id"] == entity_id


@pytest.mark.asyncio
async def test_entity_update_and_delete_enforce_access_control(store, policy):
    """Verify write/delete behavior for owner and non-owner principals."""

    alice_ctx = _context(store, policy, "alice", {"read:metadata", "write:metadata"})
    created = await _execute(
        CREATE_ENTITY_MUTATION,
        alice_ctx,
        {"input": {"kind": "sample", "name": "E2", "properties": {}}},
    )
    assert created.errors is None
    entity_id = created.data["createEntity"]["id"]

    bob_ctx = _context(store, policy, "bob", {"read:metadata", "write:metadata"})
    update_mutation = """
    mutation($id: ID!, $input: UpdateEntityInput!) {
      updateEntity(id: $id, input: $input) { id uri }
    }
    """
    denied_update = await _execute(
        update_mutation,
        bob_ctx,
        {"id": entity_id, "input": {"uri": "new-uri"}},
    )
    assert denied_update.errors
    assert "Not permitted" in denied_update.errors[0].message

    allowed_update = await _execute(
        update_mutation,
        alice_ctx,
        {
            "id": entity_id,
            "input": {"uri": "new-uri", "accessBlob": {"tags": ["team"]}},
        },
    )
    assert allowed_update.errors is None
    assert allowed_update.data["updateEntity"]["uri"] == "new-uri"

    delete_mutation = "mutation($id: ID!) { deleteEntity(id: $id) }"
    allowed_delete_for_shared_tag = await _execute(
        delete_mutation, bob_ctx, {"id": entity_id}
    )
    assert allowed_delete_for_shared_tag.errors is None
    assert allowed_delete_for_shared_tag.data["deleteEntity"] is True

    allowed_delete = await _execute(delete_mutation, alice_ctx, {"id": entity_id})
    assert allowed_delete.errors is None
    assert allowed_delete.data["deleteEntity"] is False


@pytest.mark.asyncio
async def test_link_crud_and_access_control(store, policy):
    """Exercise link create/read/update/delete with policy-based checks."""

    alice_ctx = _context(store, policy, "alice", {"read:metadata", "write:metadata"})

    s = await _execute(
        CREATE_ENTITY_MUTATION,
        alice_ctx,
        {
            "input": {
                "kind": "sample",
                "name": "S",
                "properties": {},
                "accessBlob": {"tags": ["team"]},
            }
        },
    )
    o = await _execute(
        CREATE_ENTITY_MUTATION,
        alice_ctx,
        {
            "input": {
                "kind": "sample",
                "name": "O",
                "properties": {},
                "accessBlob": {"tags": ["team"]},
            }
        },
    )
    assert s.errors is None and o.errors is None
    sid = s.data["createEntity"]["id"]
    oid = o.data["createEntity"]["id"]

    link_created = await _execute(
        CREATE_LINK_MUTATION,
        alice_ctx,
        {
            "input": {
                "subjectId": sid,
                "predicate": "relates_to",
                "objectId": oid,
                "properties": {},
            }
        },
    )
    assert link_created.errors is None
    link_id = link_created.data["createLink"]["id"]

    bob_read_ctx = _context(store, policy, "bob", {"read:metadata"})
    read_link = await _execute(
        "query($id: ID!) { link(id: $id) { id predicate } }",
        bob_read_ctx,
        {"id": link_id},
    )
    assert read_link.errors is None
    assert read_link.data["link"] is None

    update_link = """
    mutation($id: ID!, $input: UpdateLinkInput!) {
      updateLink(id: $id, input: $input) { id predicate }
    }
    """
    denied_update = await _execute(
        update_link,
        bob_read_ctx,
        {
            "id": link_id,
            "input": {"predicate": "blocked", "accessBlob": {"tags": ["team"]}},
        },
    )
    assert denied_update.errors
    assert "Not permitted" in denied_update.errors[0].message

    updated = await _execute(
        update_link,
        alice_ctx,
        {"id": link_id, "input": {"accessBlob": {"tags": ["team"]}}},
    )
    assert updated.errors is None

    read_link_shared = await _execute(
        "query($id: ID!) { link(id: $id) { id predicate } }",
        bob_read_ctx,
        {"id": link_id},
    )
    assert read_link_shared.errors is None
    assert read_link_shared.data["link"]["id"] == link_id

    delete_link = "mutation($id: ID!) { deleteLink(id: $id) }"
    denied_delete = await _execute(delete_link, bob_read_ctx, {"id": link_id})
    assert denied_delete.errors
    assert "Not permitted" in denied_delete.errors[0].message

    allowed_delete = await _execute(delete_link, alice_ctx, {"id": link_id})
    assert allowed_delete.errors is None
    assert allowed_delete.data["deleteLink"] is True


@pytest.mark.asyncio
async def test_query_paths_use_access_policy_filters(store, filter_policy):
    """Confirm list queries call policy.filters and honor AccessBlobFilter output."""

    alice_ctx = _context(
        store, filter_policy, "alice", {"read:metadata", "write:metadata"}
    )

    team = await _execute(
        CREATE_ENTITY_MUTATION,
        alice_ctx,
        {
            "input": {
                "kind": "sample",
                "name": "team-visible",
                "properties": {},
                "accessBlob": {"tags": ["team"]},
            }
        },
    )
    private = await _execute(
        CREATE_ENTITY_MUTATION,
        alice_ctx,
        {
            "input": {
                "kind": "sample",
                "name": "private-visible-to-alice",
                "properties": {},
            }
        },
    )
    assert team.errors is None and private.errors is None

    bob_ctx = _context(store, filter_policy, "bob", {"read:metadata"})
    entities_query = "query { entities { id name } }"
    result = await _execute(entities_query, bob_ctx)
    assert result.errors is None
    names = {item["name"] for item in result.data["entities"]}
    assert names == {"team-visible"}
    assert filter_policy.filter_calls >= 1


@pytest.mark.asyncio
async def test_pagination_applies_after_access_filtering(store, filter_policy):
    """
    A `limit` smaller than the number of visible rows must still return
    `limit` rows, even when invisible rows are interleaved among them.
    Filtering a fixed-size page after the fact (instead of filtering before
    LIMIT/OFFSET) would silently return fewer than `limit` rows here.
    """

    alice_ctx = _context(
        store, filter_policy, "alice", {"read:metadata", "write:metadata"}
    )

    for i in range(3):
        private = await _execute(
            CREATE_ENTITY_MUTATION,
            alice_ctx,
            {
                "input": {
                    "kind": "sample",
                    "name": f"private-{i}",
                    "properties": {},
                }
            },
        )
        assert private.errors is None
        team = await _execute(
            CREATE_ENTITY_MUTATION,
            alice_ctx,
            {
                "input": {
                    "kind": "sample",
                    "name": f"team-{i}",
                    "properties": {},
                    "accessBlob": {"tags": ["team"]},
                }
            },
        )
        assert team.errors is None

    bob_ctx = _context(store, filter_policy, "bob", {"read:metadata"})
    entities_query = "query($limit: Int!) { entities(limit: $limit) { name } }"

    result = await _execute(entities_query, bob_ctx, {"limit": 2})
    assert result.errors is None
    names = [item["name"] for item in result.data["entities"]]
    assert len(names) == 2
    assert all(name.startswith("team-") for name in names)

    result_all = await _execute(entities_query, bob_ctx, {"limit": 10})
    assert result_all.errors is None
    all_names = {item["name"] for item in result_all.data["entities"]}
    assert all_names == {"team-0", "team-1", "team-2"}


@pytest.mark.asyncio
async def test_entity_node_access_blob_trigger_rejects_both_set(store):
    """
    The database trigger is the data-integrity backstop: even calling the
    store directly (bypassing the GraphQL mutation's app-level validation)
    must fail if node_id and access_blob are both set, on insert or update.
    """
    await _insert_node(store, 1, {"tags": ["team"]})

    with pytest.raises(IntegrityError):
        await store.create_entity(
            kind="sample",
            name="bad",
            node_id=1,
            access_blob={"tags": ["team"]},
        )

    entity = await store.create_entity(
        kind="sample", name="ok", node_id=1, access_blob=None
    )
    with pytest.raises(IntegrityError):
        await store.update_entity(entity.id, access_blob={"tags": ["team"]})


@pytest.mark.asyncio
async def test_duplicate_node_kind_name_conflicts(store):
    """
    The unique index deduplicates node-bound entities on (node_id, kind, name):
    the store raises EntityConflictError on a duplicate, while free-standing
    entities (node_id NULL) are left unconstrained.
    """
    await _insert_node(store, 1, {"tags": ["team"]})

    await store.create_entity(kind="sample", name="dup", node_id=1, access_blob=None)
    with pytest.raises(EntityConflictError):
        await store.create_entity(
            kind="sample", name="dup", node_id=1, access_blob=None
        )

    # A different kind, name, or node is allowed.
    await store.create_entity(kind="other", name="dup", node_id=1, access_blob=None)

    # Free-standing entities (node_id NULL) are not deduplicated.
    await store.create_entity(
        kind="sample", name="ext", node_id=None, access_blob={"tags": ["team"]}
    )
    await store.create_entity(
        kind="sample", name="ext", node_id=None, access_blob={"tags": ["team"]}
    )


@pytest.mark.asyncio
async def test_create_entity_duplicate_returns_entity_exists(store, policy):
    """The GraphQL mutation surfaces a duplicate as an ENTITY_EXISTS error."""
    await _insert_node(store, 1, {"tags": ["team"]})
    alice_ctx = _context(store, policy, "alice", {"read:metadata", "write:metadata"})

    first = await _execute(
        CREATE_ENTITY_MUTATION,
        alice_ctx,
        {"input": {"kind": "sample", "name": "dup", "nodeId": 1}},
    )
    assert first.errors is None

    second = await _execute(
        CREATE_ENTITY_MUTATION,
        alice_ctx,
        {"input": {"kind": "sample", "name": "dup", "nodeId": 1}},
    )
    assert second.errors
    assert second.errors[0].extensions["code"] == "ENTITY_EXISTS"


@pytest.mark.asyncio
async def test_create_entity_rejects_node_id_with_access_blob(store, policy):
    """The GraphQL mutation gives a friendly error instead of a raw DB error."""
    await _insert_node(store, 1, {"tags": ["team"]})
    alice_ctx = _context(store, policy, "alice", {"read:metadata", "write:metadata"})

    result = await _execute(
        CREATE_ENTITY_MUTATION,
        alice_ctx,
        {
            "input": {
                "kind": "sample",
                "name": "bad",
                "nodeId": 1,
                "accessBlob": {"tags": ["team"]},
            }
        },
    )
    assert result.errors
    assert "access is controlled by the referenced node" in result.errors[0].message


@pytest.mark.asyncio
async def test_update_entity_rejects_setting_access_blob_on_node_linked_entity(
    store, policy
):
    await _insert_node(store, 1, {"tags": ["team"]})
    alice_ctx = _context(store, policy, "alice", {"read:metadata", "write:metadata"})

    created = await _execute(
        CREATE_ENTITY_MUTATION,
        alice_ctx,
        {"input": {"kind": "sample", "name": "linked", "nodeId": 1}},
    )
    assert created.errors is None
    entity_id = created.data["createEntity"]["id"]

    update_mutation = """
    mutation($id: ID!, $input: UpdateEntityInput!) {
      updateEntity(id: $id, input: $input) { id }
    }
    """
    result = await _execute(
        update_mutation,
        alice_ctx,
        {"id": entity_id, "input": {"accessBlob": {"tags": ["other"]}}},
    )
    assert result.errors
    assert "access is controlled by the referenced node" in result.errors[0].message


@pytest.mark.asyncio
async def test_update_entity_detaching_node_reinitializes_access_blob(store, policy):
    """
    Detaching node_id (setting it to null) with no access_blob supplied in
    the same call must not leave the entity with node_id=None AND
    access_blob=None -- that would make it invisible to everyone.
    """
    await _insert_node(store, 1, {"tags": ["team"]})
    alice_ctx = _context(store, policy, "alice", {"read:metadata", "write:metadata"})

    created = await _execute(
        CREATE_ENTITY_MUTATION,
        alice_ctx,
        {"input": {"kind": "sample", "name": "linked", "nodeId": 1}},
    )
    assert created.errors is None
    entity_id = created.data["createEntity"]["id"]

    update_mutation = """
    mutation($id: ID!, $input: UpdateEntityInput!) {
      updateEntity(id: $id, input: $input) { id }
    }
    """
    detached = await _execute(
        update_mutation, alice_ctx, {"id": entity_id, "input": {"nodeId": None}}
    )
    assert detached.errors is None

    record = await store.get_entity(entity_id)
    assert record.node_id is None
    assert record.access_blob == {"user": "alice"}


@pytest.mark.asyncio
async def test_entity_read_access_delegates_to_node_access_blob(store):
    """
    An entity with node_id set has no access_blob of its own (enforced by
    the trigger), so its visibility must be resolved from the node's
    access_blob instead.
    """
    node_policy = FakeTagPolicy({"alice": {"node_team"}, "bob": {"team"}})
    await _insert_node(store, 1, {"tags": ["node_team"]})
    entity = await store.create_entity(
        kind="sample", name="linked", node_id=1, access_blob=None
    )

    alice_ctx = _context(store, node_policy, "alice", {"read:metadata"})
    bob_ctx = _context(store, node_policy, "bob", {"read:metadata"})

    alice_read = await _execute(READ_ENTITY_QUERY, alice_ctx, {"id": entity.id})
    assert alice_read.errors is None
    assert alice_read.data["entity"]["id"] == entity.id

    bob_read = await _execute(READ_ENTITY_QUERY, bob_ctx, {"id": entity.id})
    assert bob_read.errors is None
    assert bob_read.data["entity"] is None


@pytest.mark.asyncio
async def test_entities_listing_filters_by_node_access_blob(store, filter_policy):
    """The paginated `entities` query's SQL-level access filter must also
    resolve through the node when node_id is set (not just single fetches)."""
    await _insert_node(store, 1, {"tags": ["team"]}, key="visible-node")
    await _insert_node(store, 2, {"tags": ["other"]}, key="hidden-node")
    await store.create_entity(
        kind="sample", name="node-linked-visible", node_id=1, access_blob=None
    )
    await store.create_entity(
        kind="sample", name="node-linked-hidden", node_id=2, access_blob=None
    )

    bob_ctx = _context(store, filter_policy, "bob", {"read:metadata"})
    result = await _execute("query { entities { name } }", bob_ctx)
    assert result.errors is None
    names = {item["name"] for item in result.data["entities"]}
    assert names == {"node-linked-visible"}


UPSERT_NAMESPACE_MUTATION = """
mutation($prefix: String!, $uri: String!) {
    upsertNamespace(prefix: $prefix, uri: $uri) { prefix uri }
}
"""

NAMESPACES_QUERY = "query { namespaces { prefix uri } }"


@pytest.mark.asyncio
async def test_namespaces_query_and_mutations(store, policy):
    """Namespaces are manageable and listable directly through GraphQL."""

    alice_ctx = _context(store, policy, "alice", {"read:metadata", "write:metadata"})

    upserted = await _execute(
        UPSERT_NAMESPACE_MUTATION,
        alice_ctx,
        {"prefix": "schema", "uri": "https://schema.org/"},
    )
    assert upserted.errors is None
    assert upserted.data["upsertNamespace"] == {
        "prefix": "schema",
        "uri": "https://schema.org/",
    }

    listed = await _execute(NAMESPACES_QUERY, alice_ctx)
    assert listed.errors is None
    assert listed.data["namespaces"] == [
        {"prefix": "schema", "uri": "https://schema.org/"}
    ]

    # A principal without write:metadata cannot manage namespaces.
    bob_ctx = _context(store, policy, "bob", {"read:metadata"})
    denied = await _execute(
        UPSERT_NAMESPACE_MUTATION,
        bob_ctx,
        {"prefix": "other", "uri": "https://example.org/"},
    )
    assert denied.errors
    assert "Not permitted" in denied.errors[0].message

    delete_mutation = "mutation($prefix: String!) { deleteNamespace(prefix: $prefix) }"
    deleted = await _execute(delete_mutation, alice_ctx, {"prefix": "schema"})
    assert deleted.errors is None
    assert deleted.data["deleteNamespace"] is True

    listed_after_delete = await _execute(NAMESPACES_QUERY, alice_ctx)
    assert listed_after_delete.errors is None
    assert listed_after_delete.data["namespaces"] == []


@pytest.mark.asyncio
async def test_graphql_expands_and_compacts_curies(store, policy):
    """Entity/link terms written as CURIEs round-trip through GraphQL as CURIEs,
    but are stored internally as fully-expanded IRIs."""

    alice_ctx = _context(store, policy, "alice", {"read:metadata", "write:metadata"})
    await _execute(
        UPSERT_NAMESPACE_MUTATION,
        alice_ctx,
        {"prefix": "schema", "uri": "https://schema.org/"},
    )

    create_entity_with_properties = """
    mutation($input: CreateEntityInput!) {
        createEntity(input: $input) { id properties }
    }
    """
    created = await _execute(
        create_entity_with_properties,
        alice_ctx,
        {
            "input": {
                "kind": "sample",
                "name": "E",
                "properties": {"schema:name": "hello"},
            }
        },
    )
    assert created.errors is None
    entity_id = created.data["createEntity"]["id"]
    assert created.data["createEntity"]["properties"] == {"schema:name": "hello"}

    # The store holds the fully-expanded IRI, not the raw CURIE string.
    raw_record = await store.get_entity(entity_id)
    assert raw_record.properties == {"https://schema.org/name": "hello"}

    # Reading back through GraphQL compacts it to a CURIE again.
    read_query = "query($id: ID!) { entity(id: $id) { properties } }"
    read_back = await _execute(read_query, alice_ctx, {"id": entity_id})
    assert read_back.errors is None
    assert read_back.data["entity"]["properties"] == {"schema:name": "hello"}

    create_link_with_predicate = """
    mutation($input: CreateLinkInput!) {
        createLink(input: $input) { id predicate }
    }
    """
    other = await _execute(
        create_entity_with_properties,
        alice_ctx,
        {"input": {"kind": "sample", "name": "O", "properties": {}}},
    )
    other_id = other.data["createEntity"]["id"]
    link_created = await _execute(
        create_link_with_predicate,
        alice_ctx,
        {
            "input": {
                "subjectId": entity_id,
                "predicate": "schema:relatedTo",
                "objectId": other_id,
            }
        },
    )
    assert link_created.errors is None
    assert link_created.data["createLink"]["predicate"] == "schema:relatedTo"

    raw_link = await store.get_link(link_created.data["createLink"]["id"])
    assert raw_link.predicate == "https://schema.org/relatedTo"

    # A CURIE predicate filter matches the expanded, stored predicate.
    filtered = await _execute(
        "query($p: String!) { links(predicate: $p) { predicate } }",
        alice_ctx,
        {"p": "schema:relatedTo"},
    )
    assert filtered.errors is None
    assert [link["predicate"] for link in filtered.data["links"]] == [
        "schema:relatedTo"
    ]


def test_graphql_http_route_access_control_integration(tmp_path, policy):
    """Validate HTTP GraphQL route wiring with auth dependencies and policy checks."""

    catalog = in_memory(writable_storage=str(tmp_path / "storage"))
    app = build_app(
        catalog,
        access_policy=policy,
        server_settings={"database": Database(uri="sqlite:///:memory:")},
    )

    with TestClient(app) as client:
        app.dependency_overrides[get_current_principal] = lambda: "alice"
        app.dependency_overrides[get_current_access_tags] = lambda: None
        app.dependency_overrides[get_current_scopes] = lambda: {
            "read:metadata",
            "write:metadata",
        }

        subject_response = client.post(
            "/api/graphql",
            json={
                "query": CREATE_ENTITY_MUTATION,
                "variables": {
                    "input": {
                        "kind": "sample",
                        "name": "S",
                        "properties": {},
                        "accessBlob": {"tags": ["team"]},
                    }
                },
            },
        )
        object_response = client.post(
            "/api/graphql",
            json={
                "query": CREATE_ENTITY_MUTATION,
                "variables": {
                    "input": {
                        "kind": "sample",
                        "name": "O",
                        "properties": {},
                        "accessBlob": {"tags": ["team"]},
                    }
                },
            },
        )
        assert subject_response.status_code == 200
        assert object_response.status_code == 200
        subject_payload = subject_response.json()
        object_payload = object_response.json()
        assert subject_payload.get("errors") is None
        assert object_payload.get("errors") is None
        subject_id = subject_payload["data"]["createEntity"]["id"]
        object_id = object_payload["data"]["createEntity"]["id"]

        create_link_response = client.post(
            "/api/graphql",
            json={
                "query": CREATE_LINK_MUTATION,
                "variables": {
                    "input": {
                        "subjectId": subject_id,
                        "predicate": "relates_to",
                        "objectId": object_id,
                        "properties": {},
                        "accessBlob": {"tags": ["team"]},
                    }
                },
            },
        )
        assert create_link_response.status_code == 200
        create_link_payload = create_link_response.json()
        assert create_link_payload.get("errors") is None
        link_id = create_link_payload["data"]["createLink"]["id"]

        app.dependency_overrides[get_current_principal] = lambda: "bob"
        app.dependency_overrides[get_current_access_tags] = lambda: None
        app.dependency_overrides[get_current_scopes] = lambda: {"read:metadata"}

        read_response = client.post(
            "/api/graphql",
            json={
                "query": "query($id: ID!) { link(id: $id) { id predicate } }",
                "variables": {"id": link_id},
            },
        )
        assert read_response.status_code == 200
        payload = read_response.json()
        assert payload.get("errors") is None
        assert payload["data"]["link"]["id"] == link_id

    app.dependency_overrides.clear()
