"""
Tests for the unified `tiled serve demo` tree (`tiled.examples.demo`).

The demo is built once per module (it writes a full showcase catalog plus a
provenance graph, which takes a few seconds) and shared across the read-only
assertions below.
"""

import pytest

from tiled.client import Context, from_context
from tiled.config import Authentication
from tiled.examples.demo import (
    DATASET_NAMES,
    DEMO_API_KEY,
    LINKED_CONTAINER,
    build_demo_tree,
)
from tiled.server.app import build_app

GRAPH_SUMMARY = """
{
  entities(limit: 100) { name kind uri isNodeBound }
  links(limit: 200) { predicate subjectId objectId }
  namespaces { prefix uri }
}
"""


@pytest.fixture(scope="module")
def demo_context(tmpdir_module):
    tree = build_demo_tree(
        base_url="http://127.0.0.1:8000", directory=str(tmpdir_module)
    )
    app = build_app(
        tree,
        Authentication(single_user_api_key=DEMO_API_KEY, allow_anonymous_access=True),
        {},
    )
    with Context.from_app(app, api_key=DEMO_API_KEY) as context:
        yield context


@pytest.fixture(scope="module")
def demo_client(demo_context):
    return from_context(demo_context)


@pytest.fixture(scope="module")
def graph(demo_context):
    response = demo_context.http_client.post(
        "/api/graphql", json={"query": GRAPH_SUMMARY}
    )
    response.raise_for_status()
    payload = response.json()
    assert not payload.get("errors"), payload.get("errors")
    return payload["data"]


def test_demo_serves_showcase_and_linked_datasets(demo_client):
    "The demo tree exposes the showcase containers plus the provenance datasets."
    assert LINKED_CONTAINER in demo_client
    linked = demo_client[LINKED_CONTAINER]
    assert DATASET_NAMES <= set(linked.keys())
    # The reduction pipeline ends in a 2D image, and summary is a per-frame table.
    assert linked["integrated"].shape == (200, 300)
    assert linked["summary"].structure_family == "table"


def test_demo_graph_is_seeded(graph):
    "Namespaces, entities, and links from demo_graph.json are all created."
    assert {ns["prefix"] for ns in graph["namespaces"]} >= {"prov", "schema"}
    names = {entity["name"] for entity in graph["entities"]}
    assert DATASET_NAMES <= names
    assert graph["links"], "expected the provenance links to be created"


def test_demo_dataset_entities_are_node_bound(graph):
    """Entities for datasets hosted on this server are bound to their catalog
    node and carry a URI pointing at it; external entities are unbound."""
    by_name = {entity["name"]: entity for entity in graph["entities"]}
    for name in DATASET_NAMES:
        entity = by_name[name]
        assert entity["isNodeBound"], name
        assert entity["uri"].endswith(f"/api/v1/metadata/{LINKED_CONTAINER}/{name}")
    external = [e for e in graph["entities"] if e["name"] not in DATASET_NAMES]
    assert external and not any(e["isNodeBound"] for e in external)


def test_demo_graph_predicates_are_compacted(graph):
    "Predicates round-trip through the namespace registry as CURIEs."
    predicates = {link["predicate"] for link in graph["links"]}
    assert "prov:wasDerivedFrom" in predicates
    assert not any(p.startswith("http") for p in predicates)


def test_demo_provenance_traversal(demo_context):
    "The derived datasets trace back to the measured data via wasDerivedFrom."
    query = """
    query($path: [String!]) {
      entities(nodePathParts: $path) {
        name
        outgoingLinks(predicate: "prov:wasDerivedFrom") { object { name } }
      }
    }
    """
    response = demo_context.http_client.post(
        "/api/graphql",
        json={"query": query, "variables": {"path": [LINKED_CONTAINER, "integrated"]}},
    )
    payload = response.json()
    assert not payload.get("errors"), payload.get("errors")
    (integrated,) = payload["data"]["entities"]
    assert integrated["name"] == "integrated"
    upstream = {link["object"]["name"] for link in integrated["outgoingLinks"]}
    assert upstream, "integrated should be derived from an upstream dataset"
    assert upstream <= DATASET_NAMES
