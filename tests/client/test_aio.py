"""Tests for the asynchronous client (`tiled.client.aio`).

These tests run the async client against an in-process ASGI app (with its
lifespan driven by `AsyncContext.from_app`), writing seed data through the
synchronous client where convenient and reading it back asynchronously.
"""

import asyncio

import httpx
import numpy
import pytest
import stamina

from tiled.client.aio import AsyncContext
from tiled.client.aio.constructors import from_context as aio_from_context
from tiled.client.utils import ClientError
from tiled.utils import Conflicts

rng = numpy.random.default_rng(12345)


@pytest.mark.asyncio
async def test_connect_and_server_info(app):
    context = await AsyncContext.from_app(app)
    async with context:
        assert context.server_info is not None
        assert context.server_info.library_version


@pytest.mark.asyncio
async def test_context_not_picklable(aclient):
    import pickle

    with pytest.raises(TypeError):
        pickle.dumps(aclient.context)


@pytest.mark.asyncio
async def test_array_round_trip(aclient, sync_client):
    expected = rng.random((3, 4))
    sync_client.write_array(expected, key="round_trip")
    ac = await aclient.lookup("round_trip")
    actual = await ac.read()
    numpy.testing.assert_array_equal(actual, expected)
    assert ac.shape == (3, 4)
    assert len(ac) == 3  # like numpy, the size of the zeroth axis
    assert ac.dtype == expected.dtype
    # Slicing.
    numpy.testing.assert_array_equal(
        await ac.read(numpy.s_[1:3, 0:2]), expected[1:3, 0:2]
    )


@pytest.mark.asyncio
async def test_array_write_and_patch(aclient, sync_client):
    sync_client.write_array(numpy.zeros((2, 2)), key="writable")
    ac = await aclient.lookup("writable")
    ones = numpy.ones((2, 2))
    await ac.write(ones)
    numpy.testing.assert_array_equal(await ac.read(), ones)
    # Extend via patch; the cached structure is updated.
    await ac.patch(numpy.full((1, 2), 2.0), offset=(2,), extend=True)
    assert ac.shape == (3, 2)
    numpy.testing.assert_array_equal((await ac.read())[2], [2.0, 2.0])
    # Patching beyond the extent without extend=True raises Conflicts
    # (which subclasses ValueError), as in the synchronous client.
    with pytest.raises(Conflicts):
        await ac.patch(numpy.full((1, 2), 3.0), offset=(5,))
    # A dtype mismatch is rejected client-side.
    with pytest.raises(ValueError):
        await ac.patch(numpy.ones((1, 2), dtype="int32"), offset=(3,), extend=True)


@pytest.mark.asyncio
async def test_array_split_read_is_concurrent(aclient, sync_client, monkeypatch):
    from tiled.client.aio.array import AsyncArrayClient

    expected = rng.random(1000)
    sync_client.write_array(expected, key="large")
    ac = await aclient.lookup("large")
    # Force a multi-request read, split along chunk boundaries.
    monkeypatch.setattr(AsyncArrayClient, "RESPONSE_BYTESIZE_LIMIT", 800)
    actual = await ac.read()
    numpy.testing.assert_array_equal(actual, expected)


@pytest.mark.asyncio
async def test_zero_size_read_makes_no_request(aclient, sync_client, monkeypatch):
    # NB: a zero-size array cannot currently be *created* on zarr-backed
    # storage (zarr >= 3.3 rejects zero-size chunks), so one is produced
    # here by slicing a normal array down to zero.
    sync_client.write_array(rng.random((3, 5)), key="sliced_to_zero")
    ac = await aclient.lookup("sliced_to_zero")
    sent = []
    real_send = aclient.context.send

    async def counting_send(draft):
        sent.append(draft)
        return await real_send(draft)

    monkeypatch.setattr(aclient.context, "send", counting_send)
    actual = await ac.read(numpy.s_[0:0, :])
    assert actual.shape == (0, 5)
    assert actual.size == 0
    assert sent == []  # the empty result was constructed locally


@pytest.mark.asyncio
async def test_container_iteration_and_len(aclient, sync_client):
    container = sync_client.create_container("contents")
    for i in range(3):
        container.write_array(numpy.arange(i + 1), key=f"x{i}")
    node = await aclient.lookup("contents")
    assert await node.len() == 3
    assert [key async for key in node] == ["x0", "x1", "x2"]
    items = {key: value async for key, value in node.items()}
    assert set(items) == {"x0", "x1", "x2"}
    numpy.testing.assert_array_equal(await items["x2"].read(), numpy.arange(3))
    values = [value async for value in node.values()]
    assert [value.item["id"] for value in values] == ["x0", "x1", "x2"]
    # The same, one entry per page, exercising the pagination loop.
    assert [key async for key in node.keys(page_size=1)] == ["x0", "x1", "x2"]
    assert {key async for key, _ in node.items(page_size=1)} == {"x0", "x1", "x2"}
    # Nested lookup, with slash-collapsing semantics.
    nested = await aclient.lookup("contents/x1")
    numpy.testing.assert_array_equal(await nested.read(), numpy.arange(2))
    # A client for an inner node can also be constructed directly.
    inner = await aio_from_context(aclient.context, node_path_parts=["contents"])
    assert [key async for key in inner] == ["x0", "x1", "x2"]


@pytest.mark.asyncio
async def test_lookup_missing_key_raises_keyerror(aclient):
    with pytest.raises(KeyError):
        await aclient.lookup("does-not-exist")
    assert await aclient.get("does-not-exist") is None


@pytest.mark.asyncio
async def test_metadata_round_trip(aclient, sync_client):
    sync_client.write_array(numpy.arange(3), key="md", metadata={"color": "blue"})
    node = await aclient.lookup("md")
    assert dict(node.metadata) == {"color": "blue"}
    await node.update_metadata({"color": "red", "count": 3})
    assert node.metadata["color"] == "red"
    # The change is persistent, not only local.
    await node.refresh()
    assert dict(node.metadata) == {"color": "red", "count": 3}
    await node.replace_metadata({"only": "this"})
    await node.refresh()
    assert dict(node.metadata) == {"only": "this"}


@pytest.mark.asyncio
async def test_delete(aclient, sync_client):
    sync_client.write_array(numpy.arange(3), key="doomed")
    node = await aclient.lookup("doomed")
    await node.delete(external_only=False)
    with pytest.raises(KeyError):
        await aclient.lookup("doomed")


@pytest.mark.asyncio
async def test_concurrent_reads(aclient, sync_client):
    expected = rng.random((4, 4))
    for i in range(5):
        sync_client.write_array(expected + i, key=f"gather{i}")
    nodes = await asyncio.gather(*(aclient.lookup(f"gather{i}") for i in range(5)))
    arrays = await asyncio.gather(*(node.read() for node in nodes))
    for i, actual in enumerate(arrays):
        numpy.testing.assert_array_equal(actual, expected + i)


@pytest.mark.asyncio
async def test_container_new_variation_preserves_search_state(aclient, sync_client):
    sync_client.create_container("variation")
    node = await aclient.lookup("variation")
    sorted_node = node.new_variation(sorting=[("color", -1)])
    assert sorted_node._sorting == [("color", -1)]
    # Further variations (e.g. by include_data_sources) carry the search
    # state along rather than resetting it.
    variation = sorted_node.new_variation(include_data_sources=True)
    assert variation._sorting == [("color", -1)]
    assert variation._include_data_sources is True
    refreshed = await sorted_node.include_data_sources()
    assert refreshed._sorting == [("color", -1)]


@pytest.mark.asyncio
async def test_queried_container(aclient, sync_client):
    from tiled.queries import Key

    container = sync_client.create_container("queried")
    container.write_array(numpy.arange(2), key="r", metadata={"color": "red"})
    container.write_array(numpy.arange(3), key="b", metadata={"color": "blue"})
    node = await aclient.lookup("queried")
    reds = node.new_variation(queries=[Key("color") == "red"])
    assert await reds.len() == 1
    assert [key async for key in reds] == ["r"]
    # A lookup under queries takes the search-based path: it resolves keys
    # in the filtered results only.
    # Multi-page iteration under a non-default sort exercises the offset
    # pagination scheme (the server issues cursors only for the default sort).
    by_color = node.sort(("color", 1))
    assert [key async for key in by_color.keys(page_size=1)] == ["b", "r"]
    found = await reds.lookup("r")
    numpy.testing.assert_array_equal(await found.read(), numpy.arange(2))
    with pytest.raises(KeyError):
        await reds.lookup("b")  # exists, but filtered out
    assert await reds.get("b") is None


@pytest.mark.asyncio
async def test_data_sources_and_asset_manifest(aclient, sync_client):
    sync_client.write_array(numpy.arange(3), key="sourced")
    node = await aclient.lookup("sourced")
    assert node.item["attributes"].get("data_sources") is None
    data_sources = await node.data_sources()
    assert len(data_sources) == 1
    manifest = await node.asset_manifest(data_sources)
    (asset,) = data_sources[0].assets
    # Directory assets map to a list of their contents' relative paths;
    # file assets map to None.
    assert set(manifest) == {asset.id}
    if asset.is_directory:
        assert manifest[asset.id]  # non-empty list of relative paths
    else:
        assert manifest[asset.id] is None


@pytest.mark.asyncio
async def test_create_and_write(aclient):
    container = await aclient.create_container("written", metadata={"purpose": "tests"})
    assert container.item["id"] == "written"
    assert dict(container.metadata) == {"purpose": "tests"}
    # Write an array, with a key and metadata.
    expected = rng.random((3, 4))
    ac = await container.write_array(expected, key="arr", metadata={"n": 1})
    numpy.testing.assert_array_equal(await ac.read(), expected)
    assert ac.metadata["n"] == 1
    # The node is readable through a fresh lookup too.
    numpy.testing.assert_array_equal(
        await (await aclient.lookup("written", "arr")).read(), expected
    )
    # Without a key, a unique one is generated client-side.
    anonymous = await container.write_array(expected)
    assert anonymous.item["id"]
    numpy.testing.assert_array_equal(await anonymous.read(), expected)
    # Array-likes that are not numpy arrays are coerced.
    coerced = await container.write_array([1, 2, 3], key="coerced")
    numpy.testing.assert_array_equal(await coerced.read(), numpy.array([1, 2, 3]))


@pytest.mark.asyncio
@pytest.mark.parametrize("shape", [(1000,), (40, 30)])
async def test_write_array_chunked_fan_out(aclient, monkeypatch, shape):
    from tiled.client.aio.container import AsyncContainer

    # Force splitting into chunk-sized uploads. The 2-D case exercises
    # multi-dimensional block slicing (non-contiguous middle blocks).
    monkeypatch.setattr(AsyncContainer, "_SUGGESTED_MAX_UPLOAD_SIZE", 800)
    expected = rng.random(shape)
    ac = await aclient.write_array(expected, key=f"chunked-{len(shape)}d")
    assert sum(len(dim) for dim in ac.chunks) > len(shape)  # indeed fanned out
    numpy.testing.assert_array_equal(await ac.read(), expected)


@pytest.mark.asyncio
async def test_create_conflict_adoption(aclient, monkeypatch):
    """A 409 on a retried create adopts the matching node made by the lost attempt."""
    real_send = aclient.context.send
    state = {"lose_response": False, "reach_server": False}

    async def flaky_send(draft):
        if draft.method == "POST" and state["lose_response"]:
            state["lose_response"] = False
            if state["reach_server"]:
                await real_send(draft)  # the create reaches the server...
            raise httpx.ReadError("simulated lost response")  # ...the response is lost
        return await real_send(draft)

    monkeypatch.setattr(aclient.context, "send", flaky_send)
    stamina.set_active(True)  # deactivated in conftest; the retry loop needs it
    try:
        # The lost attempt created the node; the retry's 409 Conflict refers
        # to it, and it matches the request, so it is adopted.
        state.update(lose_response=True, reach_server=True)
        adopted = await aclient.create_container("adopted", metadata={"a": 1})
        assert adopted.item["id"] == "adopted"
        assert dict(adopted.metadata) == {"a": 1}
        # A genuine collision (an existing node that does not match the
        # request) is not adopted: the 409 propagates.
        state.update(lose_response=True, reach_server=False)
        with pytest.raises(ClientError) as exc_info:
            await aclient.create_container("adopted", metadata={"other": 2})
        assert exc_info.value.response.status_code == httpx.codes.CONFLICT
    finally:
        stamina.set_active(False)


# --- Surface parity between the sync and async twins -------------------------

# Deliberate divergences of the first async version. If one of these gains an
# async counterpart, remove it here.
CUT_FROM_ASYNC = {
    "login",  # API-key auth only
    "logout",
    "metadata_revisions",
    "parent",
    "export",  # exporting (and progress bars)
    "raw_export",
    "subscribe",  # websockets
    "register",  # file-registration tooling; revisit if needed
}
# Not yet implemented in the async client. Implementing one of these (the
# plan is to implement them all) must remove it here.
PENDING_IN_ASYNC = {
    "array": set(),
    "container": set(),
    "dataframe": set(),
    "sparse": set(),
    "awkward": set(),
    "ragged": set(),
    "bytes": set(),
}
SYNC_ONLY = {
    # dask: the sync non-dask ArrayClient inherits (and overrides with eager
    # versions) the dask client's surface. The public block methods exist to
    # support dask, so the async clients omit them; chunked uploads use the
    # private `_write_block` internally.
    "array": {"compute", "read_block", "write_block"},
    # The async container is not a Mapping: no indexer views; the entry-point
    # registry is module-level (aio.dispatch), not class-level; and the
    # deprecated `write_dataframe` alias is deliberately not carried over
    # (use `write_table`).
    "container": {
        "STRUCTURE_CLIENTS_FROM_ENTRYPOINTS",
        "discover_clients_from_entrypoints",
        "items_indexer",
        "keys_indexer",
        "values_indexer",
        "write_dataframe",
    },
    "dataframe": set(),
    "sparse": {"read_block", "write_block"},
    "awkward": set(),
    "ragged": {"write_block"},
    "bytes": set(),
}
ASYNC_ONLY = {
    "array": set(),
    # Explicit coroutines replacing the Mapping dunders.
    "container": {"len", "lookup"},
    "dataframe": set(),
    "sparse": set(),
    "awkward": set(),
    "ragged": set(),
    "bytes": set(),
}


def public_surface(cls):
    return {name for name in dir(cls) if not name.startswith("_")}


@pytest.mark.parametrize(
    "family",
    ["array", "container", "dataframe", "sparse", "awkward", "ragged", "bytes"],
)
def test_surface_parity(family):
    """The async twins track the sync surface, minus documented divergences.

    This test is the ledger of those divergences: adding a method to one
    twin (or implementing a pending async counterpart) must update it.
    """
    import importlib

    sync_class_names = {
        "array": "ArrayClient",
        "container": "Container",
        "dataframe": "DataFrameClient",
        "sparse": "SparseClient",
        "awkward": "AwkwardClient",
        "ragged": "RaggedClient",
        "bytes": "BytesClient",
    }
    sync_cls = getattr(
        importlib.import_module(f"tiled.client.{family}"), sync_class_names[family]
    )
    async_cls = getattr(
        importlib.import_module(f"tiled.client.aio.{family}"),
        f"Async{sync_class_names[family]}",
    )
    sync_surface = public_surface(sync_cls)
    async_surface = public_surface(async_cls)
    expected_sync_only = (CUT_FROM_ASYNC | PENDING_IN_ASYNC[family]) & sync_surface
    expected_sync_only |= SYNC_ONLY[family]
    assert sync_surface - async_surface == expected_sync_only
    assert async_surface - sync_surface == ASYNC_ONLY[family]
