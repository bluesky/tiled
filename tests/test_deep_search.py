"""
Tests for deep search: `Container.search_deep(...)` (Python) and
`GET /api/v1/search-deep/{path}` (HTTP).

These tests are written based on discussion in issue #1368:
https://github.com/bluesky/tiled/issues/1368#issuecomment-5284649768
"""

import subprocess
import sys
import uuid
from pathlib import Path

import numpy
import pytest
import pytest_asyncio

from tiled.adapters.array import ArrayAdapter
from tiled.adapters.mapping import MapAdapter
from tiled.adapters.merged import MergedDeepSearchAdapter
from tiled.catalog import from_uri, in_memory
from tiled.client import Context, from_context, record_history
from tiled.client.register import register
from tiled.queries import Key
from tiled.server.app import build_app
from tiled.server.schemas import ContainerLinks

from .conftest import TILED_TEST_POSTGRESQL_URI
from .utils import temp_postgres

# Build a nested tree, three levels deep, with metadata for searching.
#
# root/
#   top_level_match          (sample_id="abc123", depth=1)
#   nested/
#     no_match                (depth=2)
#     images/
#       sample_042            (sample_id="abc123", depth=3)
#       sample_043            (sample_id="other", depth=3)
#   other_branch/
#     sample_099              (sample_id="abc123", depth=2)


def _nested_map_tree():
    images = MapAdapter(
        {
            "sample_042": ArrayAdapter.from_array(
                numpy.ones(3), metadata={"sample_id": "abc123", "depth": 3}
            ),
            "sample_043": ArrayAdapter.from_array(
                numpy.ones(3), metadata={"sample_id": "other", "depth": 3}
            ),
        }
    )
    nested = MapAdapter(
        {
            "no_match": ArrayAdapter.from_array(numpy.ones(3), metadata={"depth": 2}),
            "images": images,
        }
    )
    other_branch = MapAdapter(
        {
            "sample_099": ArrayAdapter.from_array(
                numpy.ones(3), metadata={"sample_id": "abc123", "depth": 2}
            ),
        }
    )
    return MapAdapter(
        {
            "top_level_match": ArrayAdapter.from_array(
                numpy.ones(3), metadata={"sample_id": "abc123", "depth": 1}
            ),
            "nested": nested,
            "other_branch": other_branch,
        }
    )


def _populate_catalog_tree(client):
    client.write_array(
        numpy.ones(3),
        key="top_level_match",
        metadata={"sample_id": "abc123", "depth": 1},
    )
    nested = client.create_container("nested")
    nested.write_array(numpy.ones(3), key="no_match", metadata={"depth": 2})
    images = nested.create_container("images")
    images.write_array(
        numpy.ones(3), key="sample_042", metadata={"sample_id": "abc123", "depth": 3}
    )
    images.write_array(
        numpy.ones(3), key="sample_043", metadata={"sample_id": "other", "depth": 3}
    )
    other_branch = client.create_container("other_branch")
    other_branch.write_array(
        numpy.ones(3), key="sample_099", metadata={"sample_id": "abc123", "depth": 2}
    )


@pytest_asyncio.fixture(
    loop_scope="module", scope="module", params=["map", "sqlite", "postgresql"]
)
async def client(request, tmpdir_module):
    if request.param == "map":
        tree = _nested_map_tree()
        app = build_app(tree)
        with Context.from_app(app) as context:
            yield from_context(context)
    elif request.param == "sqlite":
        tree = in_memory(writable_storage=str(tmpdir_module / "sqlite"))
        app = build_app(tree)
        with Context.from_app(app) as context:
            client = from_context(context)
            _populate_catalog_tree(client)
            yield client
    elif request.param == "postgresql":
        if not TILED_TEST_POSTGRESQL_URI:
            raise pytest.skip("No TILED_TEST_POSTGRESQL_URI configured")
        async with temp_postgres(TILED_TEST_POSTGRESQL_URI) as uri_with_database_name:
            subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "tiled",
                    "catalog",
                    "init",
                    uri_with_database_name,
                ],
                check=True,
                capture_output=True,
            )
            tree = from_uri(
                uri_with_database_name,
                writable_storage=str(tmpdir_module / "postgresql"),
            )
            app = build_app(tree)
            with Context.from_app(app) as context:
                client = from_context(context)
                _populate_catalog_tree(client)
                yield client
    else:
        assert False


@pytest_asyncio.fixture(loop_scope="module", scope="module")
async def mixed_client(tmpdir_module):
    """A MapAdapter root with a native child and a CatalogNodeAdapter mounted at a sub-path.

    This mirrors what `tiled.config.Config.merged_trees` produces when a
    'trees' config mounts a catalog at a path other than '/' (see issue
    https://github.com/bluesky/tiled/issues/1368). `search_deep` from
    the root must descend into the mounted catalog, not just the map-native
    part of the tree.
    """
    catalog = in_memory(writable_storage=str(tmpdir_module / "mixed"))
    tree = MapAdapter(
        {
            "map_top": ArrayAdapter.from_array(
                numpy.ones(3), metadata={"sample_id": "abc123"}
            ),
            "mounted": catalog,
        }
    )
    # config.py's Config.tree_tasks() collects these off each mounted tree
    # before merging; replicate that wiring here since we build the merged
    # MapAdapter by hand.
    tasks = {
        "startup": list(catalog.startup_tasks),
        "shutdown": list(catalog.shutdown_tasks),
        "background": list(getattr(catalog, "background_tasks", [])),
    }
    app = build_app(tree, tasks=tasks)
    with Context.from_app(app) as context:
        client = from_context(context)
        _populate_catalog_tree(client["mounted"])
        yield client


def test_search_deep_finds_matches_at_all_depths(client):
    "Matches at depth 1, 2, and 3 are all found from the root."
    results = client.search_deep(Key("sample_id") == "abc123")
    assert len(results) == 3
    found_keys = {path[-1] for path in results.keys()}
    assert found_keys == {"top_level_match", "sample_042", "sample_099"}


def test_search_deep_no_matches(client):
    "An unmatched query returns an empty Mapping, not an error."
    results = client.search_deep(Key("sample_id") == "does-not-exist")
    assert len(results) == 0
    assert list(results) == []


def test_search_deep_mapping_protocol(client):
    "DeepSearchResults behaves like a Mapping: len, iteration, .items(), .values()."
    results = client.search_deep(Key("sample_id") == "abc123")
    assert len(results) == len(list(results))
    for path, node in results.items():
        assert isinstance(path, tuple)
        assert node.metadata["sample_id"] == "abc123"
    for node in results.values():
        assert node.metadata["sample_id"] == "abc123"


def test_search_deep_getitem_tuple_key(client):
    "A path tuple relative to the search root can be used to look up a result."
    results = client.search_deep(Key("sample_id") == "abc123")
    node = results[("nested", "images", "sample_042")]
    assert node.metadata["sample_id"] == "abc123"


def test_search_deep_getitem_string_key(client):
    "A slash-delimited string path is equivalent to a tuple path."
    results = client.search_deep(Key("sample_id") == "abc123")
    assert results["nested/images/sample_042"].metadata == (
        results[("nested", "images", "sample_042")].metadata
    )


def test_search_deep_getitem_key_error(client):
    "Looking up a path that did not match the query raises KeyError."
    results = client.search_deep(Key("sample_id") == "abc123")
    with pytest.raises(KeyError):
        results[("nested", "images", "sample_043")]  # exists, but does not match
    with pytest.raises(KeyError):
        results[("does", "not", "exist")]


def test_search_deep_ancestors_relative_to_root(client):
    "Searching from a subcontainer yields paths relative to that subcontainer, not the true root."
    nested = client["nested"]
    results = nested.search_deep(Key("sample_id") == "abc123")
    assert set(results.keys()) == {("images", "sample_042")}
    # The Python API's keys are relative to the search root, but the client
    # objects returned still know their true, server-absolute path.
    assert results[("images", "sample_042")].path_parts == [
        "nested",
        "images",
        "sample_042",
    ]


@pytest.mark.asyncio(loop_scope="module")
async def test_search_deep_http_ancestors_are_server_absolute(client):
    "Unlike the Python API's keys, the raw HTTP `ancestors` are always server-absolute."
    nested = client["nested"]
    link = nested.item["links"]["search_deep"]
    response = client.context.http_client.get(
        link,
        params={
            "filter[eq][condition][key]": "sample_id",
            "filter[eq][condition][value]": '"abc123"',
        },
    )
    content = response.json()
    assert response.status_code == 200
    ancestors_by_id = {
        item["id"]: item["attributes"]["ancestors"] for item in content["data"]
    }
    assert ancestors_by_id["sample_042"] == ["nested", "images"]


def test_search_deep_max_depth(client):
    "An optional max_depth bounds how far down the tree the search descends."
    # depth=1 is direct children only -- same as .search() from the root.
    results = client.search_deep(Key("sample_id") == "abc123", max_depth=1)
    assert set(results.keys()) == {("top_level_match",)}


def test_search_deep_multiple_queries_are_anded(client):
    "Several positional queries are combined with logical AND."
    results = client.search_deep(Key("sample_id") == "abc123", Key("depth") == 3)
    assert set(results.keys()) == {("nested", "images", "sample_042")}

    results = client.search_deep(Key("sample_id") == "abc123", Key("depth") < 3)
    assert set(results.keys()) == {
        ("top_level_match",),
        ("other_branch", "sample_099"),
    }


def test_search_deep_same_key_range(client):
    "Two queries on the same key are both applied, giving a range."
    results = client.search_deep(Key("depth") > 1, Key("depth") < 3)
    assert set(results.keys()) == {
        ("nested", "no_match"),
        ("other_branch", "sample_099"),
    }


def test_search_deep_multiple_queries_with_max_depth(client):
    "max_depth is keyword-only and composes with multiple queries."
    results = client.search_deep(
        Key("sample_id") == "abc123", Key("depth") < 3, max_depth=1
    )
    assert set(results.keys()) == {("top_level_match",)}


def test_search_deep_requires_query(client):
    "Calling with no queries is an error rather than an unfiltered walk."
    with pytest.raises(TypeError):
        client.search_deep()


def test_search_deep_multiple_queries_http_params(client):
    "Every query is sent as its own filter parameter."
    results = client.search_deep(Key("sample_id") == "abc123", Key("depth") == 3)
    with record_history() as history:
        len(results)
    assert len(history.requests) == 1
    params = history.requests[0].url.params
    assert sorted(params.get_list("filter[eq][condition][key]")) == [
        "depth",
        "sample_id",
    ]


def test_search_deep_laziness(client):
    "Slicing .values() sends a single request with an explicit page[limit]."
    results = client.search_deep(Key("sample_id") == "abc123")
    with record_history() as history:
        values = results.values()[:2]
    assert len(values) == 2
    assert len(history.requests) == 1
    assert history.requests[0].url.params["page[limit]"] == "2"


@pytest.mark.asyncio(loop_scope="module")
async def test_search_deep_http_response_shape(client):
    "The raw HTTP response mirrors /search/{path} conventions."
    link = client.item["links"]["search_deep"]
    response = client.context.http_client.get(
        link,
        params={
            "filter[eq][condition][key]": "sample_id",
            "filter[eq][condition][value]": '"abc123"',
        },
    )
    content = response.json()
    assert response.status_code == 200
    assert content["meta"]["count"] == 3
    assert isinstance(content["data"], list)
    for item in content["data"]:
        assert "ancestors" in item["attributes"]
    assert "links" in content
    assert "next" in content["links"]


def test_search_deep_descends_into_mounted_subtree(mixed_client):
    "A catalog mounted under a MapAdapter root is still searched deeply."
    results = mixed_client.search_deep(Key("sample_id") == "abc123")
    found_keys = set(results.keys())
    assert ("map_top",) in found_keys
    assert ("mounted", "top_level_match") in found_keys
    assert ("mounted", "nested", "images", "sample_042") in found_keys
    assert ("mounted", "other_branch", "sample_099") in found_keys
    assert len(results) == 4


def test_search_deep_mounted_subtree_no_matches(mixed_client):
    "An unmatched query against a mixed tree returns an empty Mapping."
    results = mixed_client.search_deep(Key("sample_id") == "does-not-exist")
    assert len(results) == 0
    assert list(results) == []


def test_search_deep_mounted_subtree_max_depth(mixed_client):
    "max_depth is honored across the MapAdapter/catalog mount boundary."
    # depth=1 reaches only the map-native top-level child and the mount
    # point itself ("mounted" is a container, not a match); depth=2 reaches
    # one level into the mounted catalog.
    results = mixed_client.search_deep(Key("sample_id") == "abc123", max_depth=2)
    assert set(results.keys()) == {("map_top",), ("mounted", "top_level_match")}


def test_search_deep_mounted_subtree_multiple_queries(mixed_client):
    "AND is applied to both the map-native part and the mounted catalog."
    results = mixed_client.search_deep(Key("sample_id") == "abc123", Key("depth") < 3)
    assert set(results.keys()) == {
        ("mounted", "top_level_match"),
        ("mounted", "other_branch", "sample_099"),
    }


def test_search_deep_mounted_path_results_parent(mixed_client):
    "`.parent` of a nested result found via a scoped search resolves to the real absolute path."
    results = mixed_client["mounted"].search_deep(Key("sample_id") == "abc123")
    assert len(results) == 3

    # This result's key is relative to "mounted": ("nested", "images", "sample_042").
    # The server reports `ancestors` as server-absolute (like every other
    # endpoint), so `.parent`/`.path_parts` resolve to the true path,
    # "mounted/nested/images", not the (nonexistent) root-relative
    # "nested/images".
    nested_result = results[("nested", "images", "sample_042")]
    assert nested_result.path_parts == ["mounted", "nested", "images", "sample_042"]
    parent = nested_result.parent
    assert parent.path_parts == ["mounted", "nested", "images"]


@pytest.mark.asyncio(loop_scope="module")
async def test_search_deep_mounted_subtree_http_response_shape(mixed_client):
    "Ancestors for matches inside the mounted subtree include the mount prefix."
    link = mixed_client.item["links"]["search_deep"]
    response = mixed_client.context.http_client.get(
        link,
        params={
            "filter[eq][condition][key]": "sample_id",
            "filter[eq][condition][value]": '"abc123"',
        },
    )
    content = response.json()
    assert response.status_code == 200
    assert content["meta"]["count"] == 4
    ancestors_by_id = {
        item["id"]: item["attributes"]["ancestors"] for item in content["data"]
    }
    assert ["mounted", "nested", "images"] in ancestors_by_id.values()


_SAMPLE_ID_PARAMS = {
    "filter[eq][condition][key]": "sample_id",
    "filter[eq][condition][value]": '"abc123"',
}


@pytest.mark.parametrize("page_size", [1, 2, 3])
def test_search_deep_pagination_across_mount_boundary(mixed_client, page_size):
    "Pages ending in, starting in, or spanning the local/mount boundary drop and repeat nothing."
    results = mixed_client.search_deep(Key("sample_id") == "abc123")
    keys = [key for key, _ in results.items().page_size(page_size)]
    assert len(keys) == len(set(keys)) == 4
    # The map-native entry comes first, then the mounted catalog's entries.
    assert keys[0] == ("map_top",)
    assert all(key[0] == "mounted" for key in keys[1:])
    assert set(keys) == set(results.keys())


@pytest.mark.asyncio(loop_scope="module")
async def test_search_deep_max_depth_bounds(client):
    "max_depth must be between 1 and 64."
    link = client.item["links"]["search_deep"]
    for max_depth, expected_status in [(0, 422), (-1, 422), (65, 422), (64, 200)]:
        response = client.context.http_client.get(
            link, params={**_SAMPLE_ID_PARAMS, "max_depth": max_depth}
        )
        assert response.status_code == expected_status, max_depth


@pytest.mark.asyncio(loop_scope="module")
async def test_search_deep_sort_param_is_a_400(client):
    "Passing a sort parameter to /search-deep returns 400."
    link = client.item["links"]["search_deep"]
    response = client.context.http_client.get(
        link, params={**_SAMPLE_ID_PARAMS, "sort": "sample_id"}
    )
    assert response.status_code == 400
    assert "Sorting is not supported" in response.json()["detail"]


def test_search_deep_unsupported_server_link_is_not_implemented(client, monkeypatch):
    "A server that does not advertise the link gives a clear client-side error."
    nested = client["nested"]
    monkeypatch.delitem(nested.item["links"], "search_deep")
    with pytest.raises(NotImplementedError):
        nested.search_deep(Key("sample_id") == "abc123")


def test_container_links_search_deep_is_optional():
    "Servers that predate deep search omit the link; the schema must still validate."
    links = ContainerLinks(self="s", search="q", full="f")
    assert links.search_deep is None


class _CountingSubtree:
    "Stand-in for a mounted catalog that records how often its length is requested."

    def __init__(self, keys):
        self._keys = keys
        self.len_calls = 0

    async def exact_len(self):
        self.len_calls += 1
        return len(self._keys)

    async def keys_range(self, offset=0, limit=None):
        stop = None if limit is None else offset + limit
        return self._keys[offset:stop]


async def test_merged_deep_search_adapter_mount_length_is_lazy_and_cached():
    subtree = _CountingSubtree(["x", "y", "z"])
    adapter = MergedDeepSearchAdapter({"a": object()}, [("m", subtree)])

    assert await adapter.keys_range(0, None) == ["a", "m/x", "m/y", "m/z"]
    # No offset reached the mount, so its length was never needed.
    assert subtree.len_calls == 0

    assert await adapter.keys_range(2, 2) == ["m/y", "m/z"]
    assert await adapter.keys_range(3, 1) == ["m/z"]
    assert await adapter.exact_len() == 4
    assert subtree.len_calls == 1


def test_map_adapter_search_deep_rejects_slash_in_key():
    "Keys are joined with '/', so a key containing one would be ambiguous."
    leaf = ArrayAdapter.from_array(numpy.ones(3))
    with pytest.raises(ValueError, match="a/b"):
        MapAdapter({"a/b": leaf}).search_deep()
    with pytest.raises(ValueError, match="a/b"):
        MapAdapter({"outer": MapAdapter({"a/b": leaf})}).search_deep()


def test_search_deep_slash_in_map_key_is_a_400():
    tree = MapAdapter({"a/b": ArrayAdapter.from_array(numpy.ones(3))})
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)
        response = context.http_client.get(
            client.item["links"]["search_deep"], params=_SAMPLE_ID_PARAMS
        )
    assert response.status_code == 400


async def test_search_deep_on_data_source_backed_node_is_a_400(tmpdir):
    "Deep search does not descend into the contents of a registered file."
    h5py = pytest.importorskip("h5py")
    with h5py.File(Path(tmpdir, "data.h5"), "w") as file:
        file.create_group("g").create_dataset("d", data=numpy.ones(3))
    # Plain in_memory() catalogs share one database per process, so the
    # module-scoped fixtures above would be visible (and overwritten) here.
    catalog = in_memory(
        writable_storage=str(tmpdir), named_memory=f"deep_search_{uuid.uuid4().hex}"
    )
    with Context.from_app(build_app(catalog)) as context:
        client = from_context(context)
        await register(client, tmpdir)
        response = context.http_client.get(
            client["data"].item["links"]["search_deep"], params=_SAMPLE_ID_PARAMS
        )
    assert response.status_code == 400
