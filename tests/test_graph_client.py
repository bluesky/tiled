"""
Tests for the client-side graph-of-links helpers in `tiled.client.graph`.

These exercise the full stack: a real Tiled Python client talking to the
GraphQL route over the in-process transport, against a catalog-backed tree so
that node-bound entities and node-path resolution work.
"""

import pytest

from tiled.catalog import in_memory
from tiled.client import Context, from_context
from tiled.client.graph import (
    EntityExistsError,
    GraphClient,
    make_entity,
    make_link,
    register_namespace,
)
from tiled.server.app import build_app


@pytest.fixture
def client(tmp_path):
    catalog = in_memory(writable_storage=str(tmp_path / "storage"))
    app = build_app(catalog)
    with Context.from_app(app) as context:
        yield from_context(context)


def test_bind_entity_defaults(client):
    "bind_entity with no arguments defaults name and kind from the node."
    client.write_array([1, 2, 3], key="array1")
    array1 = client["array1"]

    entity = array1.bind_entity()

    assert entity.name == "array1"
    assert entity.kind == "array"
    assert entity.is_node_bound
    assert entity.uri == array1.uri


def test_bind_entity_explicit_name_and_kind(client):
    "Explicit name and kind override the node-derived defaults."
    client.write_array([1, 2, 3], key="cal")
    cal = client["cal"]

    entity = cal.bind_entity(name="reference", kind="calibration")

    assert entity.name == "reference"
    assert entity.kind == "calibration"


def test_bind_entity_collision_raises(client):
    "A duplicate (node, kind, name) raises EntityExistsError."
    client.write_array([1, 2, 3], key="array1")
    array1 = client["array1"]
    array1.bind_entity()

    with pytest.raises(EntityExistsError):
        array1.bind_entity()


def test_bind_entity_distinct_kind_no_collision(client):
    "The same node may carry multiple entities of distinct kind/name."
    client.write_array([1, 2, 3], key="array1")
    array1 = client["array1"]

    first = array1.bind_entity()
    second = array1.bind_entity(kind="reference")

    assert first.id != second.id
    assert {e.id for e in array1.entities()} == {first.id, second.id}


def test_entities_method_empty_by_default(client):
    "A freshly written node has no entities."
    client.write_array([1, 2, 3], key="array1")
    assert client["array1"].entities() == []


def test_make_entity_external(client):
    "make_entity creates a free-standing entity with no bound node."
    sample = make_entity(
        client,
        name="LaB6",
        kind="sample",
        uri="http://www.wikidata.org/entity/Q0",
    )
    assert sample.is_node_bound is False
    assert sample.name == "LaB6"
    assert sample.kind == "sample"
    assert sample.uri == "http://www.wikidata.org/entity/Q0"


def test_make_entity_external_duplicates_allowed(client):
    "External entities (no bound node) are not subject to uniqueness."
    first = make_entity(client, name="LaB6", kind="sample")
    second = make_entity(client, name="LaB6", kind="sample")
    assert first.id != second.id


def test_make_link_between_entities(client):
    "make_link connects two entity handles with a directed predicate."
    client.write_array([1, 2, 3], key="measured")
    client.write_array([0, 1, 2], key="subtracted")
    measured = client["measured"].bind_entity()
    subtracted = client["subtracted"].bind_entity()

    link = make_link(
        subject=subtracted, object=measured, predicate="prov:wasDerivedFrom"
    )

    assert link.subject_id == subtracted.id
    assert link.object_id == measured.id


def test_register_namespace_roundtrip(client):
    "A registered namespace prefix appears in the namespaces mapping."
    register_namespace(client, "ex", "http://example.org/")
    graph = GraphClient(client.context)
    assert graph.namespaces["ex"] == "http://example.org/"


def test_graphclient_entities_filters(client):
    "GraphClient.find_entities filters by kind, node, and name."
    client.write_array([1, 2, 3], key="array1")
    array1 = client["array1"]
    entity = array1.bind_entity()
    graph = GraphClient(client.context)

    assert [e.id for e in graph.find_entities(kind="array")] == [entity.id]
    assert [e.id for e in graph.find_entities(name="array1")] == [entity.id]
    assert graph.find_entities(name="does-not-exist") == []


def test_entity_lookup_by_id(client):
    "GraphClient.get_entity fetches a single entity by id."
    client.write_array([1, 2, 3], key="array1")
    entity = client["array1"].bind_entity()
    graph = GraphClient(client.context)

    fetched = graph.get_entity(entity.id)
    assert fetched is not None
    assert fetched.id == entity.id
    assert graph.get_entity("00000000-0000-0000-0000-000000000000") is None


def test_delete_entity_by_id(client):
    "GraphClient.delete_entity removes an entity by id."
    client.write_array([1, 2, 3], key="array1")
    entity = client["array1"].bind_entity()
    graph = GraphClient(client.context)

    assert graph.delete_entity(entity.id) is True
    assert graph.get_entity(entity.id) is None
    # Deleting again reports that nothing was removed.
    assert graph.delete_entity(entity.id) is False


def test_entity_handle_delete_delegates(client):
    "EntityHandle.delete removes the entity through its GraphClient."
    client.write_array([1, 2, 3], key="array1")
    entity = client["array1"].bind_entity()
    graph = GraphClient(client.context)

    assert entity.delete() is True
    assert graph.get_entity(entity.id) is None


def test_delete_link_by_id(client):
    "GraphClient.delete_link removes a link by id."
    client.write_array([1, 2, 3], key="measured")
    client.write_array([0, 1, 2], key="subtracted")
    measured = client["measured"].bind_entity()
    subtracted = client["subtracted"].bind_entity()
    link = make_link(
        subject=subtracted, object=measured, predicate="prov:wasDerivedFrom"
    )
    graph = GraphClient(client.context)

    assert graph.delete_link(link.id) is True
    # Deleting again reports that nothing was removed.
    assert graph.delete_link(link.id) is False
