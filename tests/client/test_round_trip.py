"""Write/read round trips run identically through the sync and async clients.

Each test body is written once and parametrized over the two client
flavors. The `call` helper awaits a result only when there is one to
await, so the same lines drive `Container.write_array(...)` and
`await AsyncContainer.write_array(...)` alike. Where the APIs
deliberately diverge (`len(c)` vs `await c.len()`, iteration), small
helpers below bridge the difference.
"""

import inspect

import numpy
import pandas
import pandas.testing
import pytest

from tiled.queries import Key

rng = numpy.random.default_rng(42)

FLAVORS = ["sync", "aio"]


async def call(fn, /, *args, **kwargs):
    "Call a sync or async callable uniformly: await the result if awaitable."
    result = fn(*args, **kwargs)
    if inspect.isawaitable(result):
        result = await result
    return result


async def to_list(iterable):
    "Collect a sync iterable or an async iterator into a list."
    if hasattr(iterable, "__aiter__"):
        return [item async for item in iterable]
    return list(iterable)


async def length_of(container):
    "The number of entries: `len(c)` for sync, `await c.len()` for async."
    if hasattr(container, "len"):
        return await container.len()
    return len(container)


@pytest.fixture(params=FLAVORS)
def flavor(request):
    return request.param


@pytest.fixture
def client(flavor, sync_client, aclient):
    return {"sync": sync_client, "aio": aclient}[flavor]


@pytest.mark.asyncio
async def test_round_trip_array(flavor, client):
    expected = rng.random((3, 4))
    node = await call(
        client.write_array, expected, key=f"arr-{flavor}", metadata={"n": 1}
    )
    numpy.testing.assert_array_equal(await call(node.read), expected)
    numpy.testing.assert_array_equal(
        await call(node.read, numpy.s_[1:, :2]), expected[1:, :2]
    )
    assert node.metadata["n"] == 1
    assert node.shape == (3, 4)
    # Overwrite, and read back through a fresh lookup.
    await call(node.write, expected + 1)
    again = await call(client.get, f"arr-{flavor}")
    numpy.testing.assert_array_equal(await call(again.read), expected + 1)


@pytest.mark.asyncio
async def test_round_trip_table(flavor, client):
    expected = pandas.DataFrame({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})
    node = await call(client.write_table, expected, key=f"table-{flavor}")
    pandas.testing.assert_frame_equal(await call(node.read), expected)
    pandas.testing.assert_frame_equal(await call(node.read_partition, 0), expected)
    pandas.testing.assert_frame_equal(
        await call(node.read, ["a"]), expected[["a"]]
    )
    assert list(node.columns) == ["a", "b"]
    assert await to_list(node) == ["a", "b"]
    with pytest.raises(IndexError):
        await call(node.read_partition, 99)


@pytest.mark.asyncio
async def test_round_trip_appendable_table(flavor, client):
    import pyarrow

    expected = pyarrow.Table.from_pydict({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})
    node = await call(
        client.create_appendable_table, expected.schema, key=f"appendable-{flavor}"
    )
    await call(node.append_partition, 0, expected)
    await call(node.append_partition, 0, expected)
    actual = await call(node.read)
    assert len(actual) == 2 * expected.num_rows
    pandas.testing.assert_frame_equal(
        actual.reset_index(drop=True),
        pyarrow.concat_tables([expected, expected]).to_pandas(),
        check_dtype=False,
    )


@pytest.mark.asyncio
async def test_round_trip_sparse(flavor, client):
    import sparse

    coo = sparse.COO(
        coords=numpy.array([[2, 5]]), data=numpy.array([1.3, 7.5]), shape=(10,)
    )
    node = await call(
        client.write_sparse,
        coords=coo.coords,
        data=coo.data,
        shape=coo.shape,
        key=f"sparse-{flavor}",
    )
    actual = await call(node.read)
    numpy.testing.assert_array_equal(actual.todense(), coo.todense())
    numpy.testing.assert_array_equal(await call(node.todense), coo.todense())
    assert node.shape == (10,)


@pytest.mark.asyncio
async def test_round_trip_awkward(flavor, client):
    import awkward

    expected = awkward.Array([[1, 2, 3], [], [4, 5]])
    node = await call(client.write_awkward, expected, key=f"awkward-{flavor}")
    actual = await call(node.read)
    assert actual.tolist() == expected.tolist()
    sliced = await call(node.read, numpy.s_[0])
    assert sliced.tolist() == [1, 2, 3]


@pytest.mark.asyncio
@pytest.mark.parametrize("max_partition_bytes", [None, 64])
async def test_round_trip_ragged(flavor, client, max_partition_bytes):
    # With a tiny max_partition_bytes the upload is split into blocks
    # (sequential in the sync client, concurrent in the async one).
    expected = [[1.0, 2.0], [3.0], [4.0, 5.0, 6.0], [7.0]]
    key = f"ragged-{flavor}-{max_partition_bytes}"
    node = await call(
        client.write_ragged,
        expected,
        key=key,
        max_partition_bytes=max_partition_bytes,
    )
    assert (await call(node.read)).tolist() == expected
    # Append via patch; the cached structure (shape) is updated.
    await call(node.patch, [[8.0, 9.0]], offset=(4,), extend=True)
    assert (await call(node.read)).tolist() == expected + [[8.0, 9.0]]
    assert node.shape[0] == 5


@pytest.mark.asyncio
async def test_round_trip_container(flavor, client):
    parent = await call(
        client.create_container, f"cont-{flavor}", metadata={"title": "parent"}
    )
    await call(parent.write_array, numpy.arange(2), key="r", metadata={"color": "red"})
    await call(parent.write_array, numpy.arange(3), key="b", metadata={"color": "blue"})
    assert await length_of(parent) == 2
    assert await to_list(parent.keys()) == ["r", "b"]

    # Metadata mutation.
    await call(parent.update_metadata, {"title": "updated"})
    await call(parent.refresh)
    assert parent.metadata["title"] == "updated"

    # search() and sort() are shared (pure) and honored by both clients.
    reds = parent.search(Key("color") == "red")
    assert await length_of(reds) == 1
    assert await to_list(reds.keys()) == ["r"]
    by_color = parent.sort(("color", 1))
    assert await to_list(by_color.keys()) == ["b", "r"]
    by_color_desc = parent.sort(("color", -1))
    assert await to_list(by_color_desc.keys()) == ["r", "b"]

    # distinct()
    distinct = await call(parent.distinct, "color", counts=True)
    colors = {
        entry["value"]: entry["count"] for entry in distinct["metadata"]["color"]
    }
    assert colors == {"red": 1, "blue": 1}

    # delete_contents()
    await call(parent.delete_contents, "r", external_only=False)
    assert await to_list(parent.keys()) == ["b"]


@pytest.mark.asyncio
async def test_round_trip_search_deep(flavor, client):
    parent = await call(client.create_container, f"deep-{flavor}")
    inner = await call(parent.create_container, "inner")
    await call(
        inner.write_array, numpy.arange(3), key="target", metadata={"deep_tag": flavor}
    )
    await call(
        parent.write_array, numpy.arange(2), key="shallow", metadata={"deep_tag": flavor}
    )
    results = parent.search_deep(Key("deep_tag") == flavor)
    assert await length_of(results) == 2
    keys = await to_list(results.keys())
    assert set(keys) == {("inner", "target"), ("shallow",)}
    # Values are live clients; keys are relative to the searched node.
    found = await call(results.get, ("inner", "target"))
    numpy.testing.assert_array_equal(await call(found.read), numpy.arange(3))
    assert await call(results.get, ("no", "such", "key")) is None
    # Narrower search: limit the depth.
    shallow_only = parent.search_deep(Key("deep_tag") == flavor, max_depth=1)
    assert await to_list(shallow_only.keys()) == [("shallow",)]
    # At least one query is required.
    with pytest.raises(TypeError):
        parent.search_deep()
