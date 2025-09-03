import random
import string
from contextlib import closing
from dataclasses import asdict
from typing import cast

import dask.array
import numpy
import pandas
import pandas.testing
import pyarrow
import pytest
import pytest_asyncio
import sqlalchemy.exc
import tifffile
import xarray
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import AsyncAdaptedQueuePool, QueuePool, StaticPool

from ..adapters.csv import CSVAdapter
from ..adapters.dataframe import ArrayAdapter
from ..adapters.tiff import TiffAdapter
from ..catalog import in_memory
from ..catalog.adapter import WouldDeleteData
from ..catalog.explain import record_explanations
from ..client import Context, from_context
from ..client.register import register
from ..client.utils import ClientError
from ..client.xarray import write_xarray_dataset
from ..queries import Eq, Key
from ..server.app import build_app, build_app_from_config
from ..server.schemas import Asset, DataSource, Management
from ..storage import SQLStorage, get_storage, parse_storage, sanitize_uri
from ..structures.core import StructureFamily
from ..utils import Conflicts, ensure_specified_sql_driver, ensure_uri
from .utils import sql_table_exists


@pytest_asyncio.fixture
async def a(adapter):
    "Raw adapter, not to be used within an app because it is manually started and stopped."
    await adapter.startup()
    yield adapter
    await adapter.shutdown()


@pytest_asyncio.fixture
async def client(adapter):
    app = build_app(adapter)
    with Context.from_app(app) as context:
        yield from_context(context)


@pytest.mark.asyncio
async def test_nested_node_creation(a):
    await a.create_node(
        key="b",
        metadata={},
        structure_family=StructureFamily.container,
        specs=[],
    )
    b = await a.lookup_adapter(["b"])
    await b.create_node(
        key="c",
        metadata={},
        structure_family=StructureFamily.container,
        specs=[],
    )
    c = await b.lookup_adapter(["c"])
    assert await b.path_segments() == ["b"]
    assert await c.path_segments() == ["b", "c"]
    assert (await a.keys_range(0, 1)) == ["b"]
    assert (await b.keys_range(0, 1)) == ["c"]
    # smoke test
    await a.items_range(0, 1)
    await b.items_range(0, 1)
    await a.shutdown()


@pytest.mark.asyncio
async def test_sorting(a):
    # Generate lists of letters and numbers, randomly shuffled.
    random_state = random.Random(0)
    ordered_letters = list(string.ascii_lowercase[:10])
    shuffled_letters = list(ordered_letters)
    random_state.shuffle(shuffled_letters)
    shuffled_numbers = [0] * 5 + [1] * 5
    random_state.shuffle(shuffled_numbers)
    assert ordered_letters != shuffled_letters
    assert sorted(shuffled_letters) != shuffled_letters

    for letter, number in zip(shuffled_letters, shuffled_numbers):
        await a.create_node(
            key=letter,
            metadata={"letter": letter, "number": number},
            structure_family=StructureFamily.container,
            specs=[],
        )

    # Default sorting is _not_ ordered.
    default_key_order = await a.keys_range(0, 10)
    assert default_key_order != ordered_letters
    # Sorting by ("", -1) gives reversed default order.
    reversed_default_key_order = await a.sort([("", -1)]).keys_range(0, 10)
    assert reversed_default_key_order == list(reversed(default_key_order))

    # Sort by key.
    assert await a.sort([("id", 1)]).keys_range(0, 10) == ordered_letters
    # Test again, with items_range.
    assert [
        k for k, v in await a.sort([("id", 1)]).items_range(0, 10)
    ] == ordered_letters

    # Sort by letter metadata.
    # Use explicit 'metadata.{key}' namespace.
    assert (await a.sort([("metadata.letter", 1)]).keys_range(0, 10)) == ordered_letters

    # Sort by letter metadata.
    # Use implicit '{key}' (more convenient, and necessary for back-compat).
    assert (await a.sort([("letter", 1)]).keys_range(0, 10)) == ordered_letters

    # Sort by number and then by letter.
    # Use explicit 'metadata.{key}' namespace.
    items = await a.sort([("metadata.number", 1), ("metadata.letter", 1)]).items_range(
        0, 10
    )
    numbers = [v.metadata()["number"] for k, v in items]
    letters = [v.metadata()["letter"] for k, v in items]
    keys = [k for k, v in items]
    # Numbers are sorted.
    numbers = sorted(numbers)
    # Within each block of numbers, keys and letters are sorted.
    assert sorted(keys[:5]) == keys[:5] == letters[:5]
    assert sorted(keys[5:]) == keys[5:] == letters[5:]


@pytest.mark.asyncio
async def test_search(a):
    for letter, number in zip(string.ascii_lowercase[:5], range(5)):
        await a.create_node(
            key=letter,
            metadata={"letter": letter, "number": number, "x": {"y": {"z": letter}}},
            structure_family=StructureFamily.container,
            specs=[],
        )
    assert "c" in await a.keys_range(0, 5)
    assert await a.search(Eq("letter", "c")).keys_range(0, 5) == ["c"]
    assert await a.search(Eq("number", 2)).keys_range(0, 5) == ["c"]

    # Looking up "d" inside search results should find nothing when
    # "d" is filtered out by a search query first.
    assert await a.lookup_adapter(["d"]) is not None
    with pytest.raises(KeyError):
        await a.search(Eq("letter", "c")).lookup_adapter(["d"])

    # Search on nested key.
    assert await a.search(Eq("x.y.z", "c")).keys_range(0, 5) == ["c"]

    # Created nested nodes and search on them.
    d = await a.lookup_adapter(["d"])
    for letter, number in zip(string.ascii_lowercase[:5], range(10, 15)):
        await d.create_node(
            key=letter,
            metadata={"letter": letter, "number": number},
            structure_family=StructureFamily.container,
            specs=[],
        )
    assert await d.search(Eq("letter", "c")).keys_range(0, 5) == ["c"]
    assert await d.search(Eq("number", 12)).keys_range(0, 5) == ["c"]


@pytest.mark.asyncio
async def test_metadata_index_is_used(example_data_adapter):
    a = example_data_adapter  # for succinctness below
    # Check that an index (specifically the 'top_level_metadata' index) is used
    # by inspecting the content of an 'EXPLAIN ...' query. The exact content
    # is intended for humans and is not an API, but we can coarsely check
    # that the index of interest is mentioned.
    await a.startup()
    with record_explanations() as e:
        results = await a.search(Key("number_as_string") == "3").keys_range(0, 5)
        assert len(results) == 1
        assert "top_level_metadata" in str(e)
    with record_explanations() as e:
        results = await a.search(Key("number") == 3).keys_range(0, 5)
        assert len(results) == 1
        assert "top_level_metadata" in str(e)
    with record_explanations() as e:
        results = await a.search(Key("bool") == False).keys_range(0, 5)  # noqa: #712
        assert len(results) == 1
        assert "top_level_metadata" in str(e)
    with record_explanations() as e:
        results = await a.search(Key("nested.number_as_string") == "3").keys_range(0, 5)
        assert len(results) == 1
        assert "top_level_metadata" in str(e)
    with record_explanations() as e:
        results = await a.search(Key("nested.number") == 3).keys_range(0, 5)
        assert len(results) == 1
        assert "top_level_metadata" in str(e)
    with record_explanations() as e:
        results = await a.search(Key("nested.bool") == False).keys_range(  # noqa: #712
            0, 5
        )
        assert len(results) == 1
        assert "top_level_metadata" in str(e)
    await a.shutdown()


@pytest.mark.asyncio
async def test_write_array_external(a, tmpdir):
    arr = numpy.ones((5, 3))
    filepath = str(tmpdir / "file.tiff")
    data_uri = ensure_uri(filepath)
    tifffile.imwrite(filepath, arr)
    ad = TiffAdapter(data_uri)
    structure = asdict(ad.structure())
    await a.create_node(
        key="x",
        structure_family="array",
        metadata={},
        data_sources=[
            DataSource(
                structure_family="array",
                mimetype="image/tiff",
                structure=structure,
                parameters={},
                management="external",
                assets=[
                    Asset(
                        parameter="data_uri",
                        num=None,
                        data_uri=str(data_uri),
                        is_directory=False,
                    )
                ],
            )
        ],
    )
    x = await a.lookup_adapter(["x"])
    assert numpy.array_equal(await x.read(), arr)


@pytest.mark.asyncio
async def test_write_dataframe_external_direct(a, tmpdir):
    df = pandas.DataFrame(numpy.ones((5, 3)), columns=list("abc"))
    filepath = str(tmpdir / "file.csv")
    data_uri = ensure_uri(filepath)
    df.to_csv(filepath, index=False)
    dfa = CSVAdapter.from_uris(data_uri)
    structure = asdict(dfa.structure())
    await a.create_node(
        key="x",
        structure_family=StructureFamily.table,
        metadata={},
        data_sources=[
            DataSource(
                structure_family="table",
                mimetype="text/csv",
                structure=structure,
                parameters={},
                management="external",
                assets=[
                    Asset(
                        parameter="data_uris",
                        num=0,
                        data_uri=data_uri,
                        is_directory=False,
                    )
                ],
            )
        ],
    )
    x = await a.lookup_adapter(["x"])
    pandas.testing.assert_frame_equal(await x.read(), df)


@pytest.mark.asyncio
async def test_write_array_internal_direct(a, tmpdir):
    from ..media_type_registration import default_deserialization_registry

    arr = numpy.ones((5, 3))
    ad = ArrayAdapter.from_array(arr)
    structure = ad.structure()
    await a.create_node(
        key="x",
        structure_family="array",
        metadata={},
        data_sources=[
            DataSource(
                structure_family="array",
                structure=structure,
                management="writable",
            )
        ],
    )
    x = await a.lookup_adapter(["x"])

    media_type = "application/octet-stream"
    body = arr.tobytes()
    deserializer = default_deserialization_registry.dispatch("array", media_type)
    await x.write(media_type, deserializer, x, body)

    val = await x.read()
    assert numpy.array_equal(val, arr)


def test_write_array_internal_via_client(client):
    expected = numpy.array([1, 3, 7])
    x = client.write_array(expected)
    actual = x.read()
    assert numpy.array_equal(actual, expected)

    y = client.write_array(dask.array.from_array(expected, chunks=((1, 1, 1),)))
    actual = y.read()
    assert numpy.array_equal(actual, expected)


def test_write_dataframe_internal_via_client(client):
    expected = pandas.DataFrame(numpy.ones((5, 3)), columns=list("abc"))
    x = client.write_dataframe(expected)
    actual = x.read()
    pandas.testing.assert_frame_equal(actual, expected)

    # y = client.write_array(dask.array.from_array(expected, chunks=((1, 1, 1),)))
    # actual = y.read()
    # assert numpy.array_equal(actual, expected)
    # pandas.testing.assert_frame_equal(actual, expected)


def test_write_xarray_dataset(client):
    ds = xarray.Dataset(
        {"temp": (["time"], numpy.array([101, 102, 103]))},
        coords={"time": (["time"], numpy.array([1, 2, 3]))},
    )
    dsc = write_xarray_dataset(client, ds, key="test_xarray_dataset")
    assert set(dsc) == {"temp", "time"}
    # smoke test
    dsc["temp"][:]
    dsc["time"][:]
    dsc.read()


@pytest.mark.asyncio
async def test_delete_catalog_tree(tmpdir):
    # Do not use client fixture here.
    # The Context must be opened inside the test or we run into
    # event loop crossing issues with the Postgres test.
    tree = in_memory(writable_storage=str(tmpdir))
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)

        a = client.create_container("a")
        b = a.create_container("b")
        b.write_array([1, 2, 3])
        b.write_array([4, 5, 6])
        c = b.create_container("c")
        d = c.create_container("d")
        d.write_array([7, 8, 9])

        nodes_before_delete = (await tree.context.execute("SELECT * from nodes")).all()
        assert len(nodes_before_delete) == 7 + 1  # +1 for the root node
        data_sources_before_delete = (
            await tree.context.execute("SELECT * from data_sources")
        ).all()
        assert len(data_sources_before_delete) == 3
        assets_before_delete = (
            await tree.context.execute("SELECT * from assets")
        ).all()
        assert len(assets_before_delete) == 3

        with pytest.raises(Conflicts, match="Cannot delete a node that is not empty."):
            await tree.delete()

        with pytest.raises(WouldDeleteData):
            await tree.delete(recursive=True)  # external_only=True by default
        with pytest.raises(WouldDeleteData):
            await tree.delete(recursive=True, external_only=True)
        await tree.delete(recursive=True, external_only=False)

        nodes_after_delete = (await tree.context.execute("SELECT * from nodes")).all()
        assert len(nodes_after_delete) == 0 + 1  # the root node that should remain
        data_sources_after_delete = (
            await tree.context.execute("SELECT * from data_sources")
        ).all()
        assert len(data_sources_after_delete) == 0
        assets_after_delete = (await tree.context.execute("SELECT * from assets")).all()
        assert len(assets_after_delete) == 0


@pytest.mark.asyncio
async def test_delete_contents(tmpdir):
    # Do not use client fixture here.
    # The Context must be opened inside the test or we run into
    # event loop crossing issues with the Postgres test.
    tree = in_memory(writable_storage=str(tmpdir))
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)

        # a has children b1 and b2, which each contain arrays
        a = client.create_container("a")
        b1 = a.create_container("b1")
        b1.write_array([1, 2, 3], key="test_1")
        b1.write_array([4, 5, 6], key="test_2")
        b1.write_array([7, 8, 9], key="test_3")
        b2 = a.create_container("b2")
        b2.write_array([10, 11, 12], key="test_4")
        b2.write_array([13, 14, 15], key="test_5")
        a.create_container("b3")  # empty container

        assert set(client) == {"a"}
        assert set(client["a"]) == {"b1", "b2", "b3"}
        assert set(client["a"]["b1"]) == {"test_1", "test_2", "test_3"}
        assert set(client["a"]["b2"]) == {"test_4", "test_5"}

        # Check the database state before deletion
        nodes_before_delete = (await tree.context.execute("SELECT * from nodes")).all()
        assert len(nodes_before_delete) == 9 + 1
        data_sources_before_delete = (
            await tree.context.execute("SELECT * from data_sources")
        ).all()
        assert len(data_sources_before_delete) == 5
        assets_before_delete = (
            await tree.context.execute("SELECT * from assets")
        ).all()
        assert len(assets_before_delete) == 5

        # Trying to delete a non-empty node without recursive=True should raise
        with pytest.raises(
            ClientError, match="Cannot delete a node that is not empty."
        ):
            client["a"].delete_contents(["b1"], recursive=False, external_only=True)

        # Trying to delete internal data with external_only=True should raise
        with pytest.raises(
            ClientError, match="Some items in this tree are internally managed."
        ):
            client["a"].delete_contents(["b1"], recursive=True, external_only=True)

        # Delete arrays from b1 (as a scalar and as a list), and then b1 itself
        b1.delete_contents("test_1", external_only=False)
        assert set(client["a"]["b1"].keys()) == {"test_2", "test_3"}
        b1.delete_contents(["test_2", "test_3"], external_only=False)
        assert set(client["a"]["b1"].keys()) == set()
        client["a"].delete_contents(["b1"], recursive=False, external_only=True)
        assert set(client["a"]) == {"b2", "b3"}

        # Delete all contents of a, including the non-empty b2 and the empty b3
        client["a"].delete_contents(external_only=False, recursive=True)
        assert set(client["a"]) == set()

        # Check the database state; only a and the root node should remain.
        nodes_after_delete = (await tree.context.execute("SELECT * from nodes")).all()
        assert len(nodes_after_delete) == 1 + 1
        data_sources_after_delete = (
            await tree.context.execute("SELECT * from data_sources")
        ).all()
        assert len(data_sources_after_delete) == 0
        assets_after_delete = (await tree.context.execute("SELECT * from assets")).all()
        assert len(assets_after_delete) == 0


@pytest.mark.asyncio
async def test_delete_with_external_nodes(tmpdir):
    # Do not use client fixture here.
    # The Context must be opened inside the test or we run into
    # event loop crossing issues with the Postgres test.
    (tmpdir / "readable").mkdir()
    (tmpdir / "writable").mkdir()
    tree = in_memory(
        readable_storage=[str(tmpdir / "readable")],
        writable_storage={"filesystem": str(tmpdir / "writable")},
    )

    # Create some external data to register
    for i in range(1, 5):
        with open(tmpdir / "readable" / f"test_{i}.csv", "w") as file:
            file.write(
                """a, b, c
                    1, 2, 3
                    4, 5, 6
                """
            )
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)

        # a has children b1 and b2, which each contain arrays
        a = client.create_container("a")
        b1 = a.create_container("b1")
        await register(b1, tmpdir / "readable" / "test_1.csv")
        await register(b1, tmpdir / "readable" / "test_2.csv")
        b2 = a.create_container("b2")
        await register(b2, tmpdir / "readable" / "test_3.csv")
        await register(b2, tmpdir / "readable" / "test_4.csv")

        assert list(client) == ["a"]
        assert list(client["a"]) == ["b1", "b2"]
        assert list(client["a"]["b1"]) == ["test_1", "test_2"]
        assert list(client["a"]["b2"]) == ["test_3", "test_4"]

        nodes_before_delete = (await tree.context.execute("SELECT * from nodes")).all()
        assert len(nodes_before_delete) == 7 + 1  # +1 for the root node
        data_sources_before_delete = (
            await tree.context.execute("SELECT * from data_sources")
        ).all()
        assert len(data_sources_before_delete) == 4
        assets_before_delete = (
            await tree.context.execute("SELECT * from assets")
        ).all()
        assert len(assets_before_delete) == 4

        # Delete all children of b1, and b1 itself.
        client["a"].delete_contents("b1", recursive=True)

        assert list(client) == ["a"]
        assert list(client["a"]) == ["b2"]
        assert list(client["a"]["b2"]) == ["test_3", "test_4"]  # not affected
        nodes_after_delete = (await tree.context.execute("SELECT * from nodes")).all()
        assert len(nodes_after_delete) == 4 + 1  # +1 for the root node
        data_sources_after_delete = (
            await tree.context.execute("SELECT * from data_sources")
        ).all()
        assert len(data_sources_after_delete) == 2
        assets_after_delete = (await tree.context.execute("SELECT * from assets")).all()
        assert len(assets_after_delete) == 2


@pytest.mark.asyncio
async def test_delete_sql_assets(sql_storage_uri):
    # Do not use client fixture here.
    # The Context must be opened inside the test or we run into
    # event loop crossing issues with the Postgres test.

    tree = in_memory(writable_storage={"sql": sql_storage_uri})
    storage = cast(SQLStorage, get_storage(parse_storage(sql_storage_uri).uri))

    # Create some tables to write
    table_1 = pyarrow.Table.from_pydict({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})
    table_2 = pyarrow.Table.from_pydict({"c": [4, 5, 6], "d": ["7", "8", "9"]})

    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)

        # a has children b1 and b2, which each contain arrays
        a = client.create_container("a")
        b1 = a.create_container("b1")
        t1 = b1.create_appendable_table(schema=table_1.schema, key="table_1")
        t1.append_partition(table_1, 0)
        t1.append_partition(table_1, 0)
        t2 = b1.create_appendable_table(schema=table_2.schema, key="table_2")
        t2.append_partition(table_2, 0)
        assert t1.read() is not None
        assert t2.read() is not None

        # Check the SQL storage directly
        t1_table_name = t1.data_sources()[0].parameters["table_name"]
        t1_dataset_id = t1.data_sources()[0].parameters["dataset_id"]
        t2_table_name = t2.data_sources()[0].parameters["table_name"]
        t2_dataset_id = t2.data_sources()[0].parameters["dataset_id"]
        with closing(storage.connect()) as conn:
            assert sql_table_exists(conn, storage.dialect, t1_table_name)
            assert sql_table_exists(conn, storage.dialect, t2_table_name)
            with conn.cursor() as cursor:
                cursor.execute(
                    f'SELECT COUNT(*) FROM "{t1_table_name}" '
                    f"WHERE _dataset_id = {t1_dataset_id:d};",
                )
                assert cursor.fetchone()[0] == 6
                cursor.execute(
                    f'SELECT COUNT(*) FROM "{t2_table_name}" '
                    f"WHERE _dataset_id = {t2_dataset_id:d};",
                )
                assert cursor.fetchone()[0] == 3

        # Add another table to b2 -- a copy of table_1 with the same schema
        b2 = a.create_container("b2")
        t1c = b2.create_appendable_table(schema=table_1.schema, key="table_1_copy")
        t1c.append_partition(table_1, 0)
        assert t1c.read() is not None

        # Check the catalog state before deletion
        assert list(client) == ["a"]
        assert list(client["a"]) == ["b1", "b2"]
        assert list(client["a"]["b1"]) == ["table_1", "table_2"]
        assert list(client["a"]["b2"]) == ["table_1_copy"]

        # Check the number of nodes, data sources, and assets
        nodes_before_delete = (await tree.context.execute("SELECT * from nodes")).all()
        assert len(nodes_before_delete) == 6 + 1  # +1 for the root node
        data_sources_before_delete = (
            await tree.context.execute("SELECT * from data_sources")
        ).all()
        assert len(data_sources_before_delete) == 3
        assets_before_delete = (
            await tree.context.execute("SELECT * from assets")
        ).all()
        assert len(assets_before_delete) == 1  # single sql asset

        # Check the SQL storage directly
        t1c_table_name = t1c.data_sources()[0].parameters["table_name"]
        t1c_dataset_id = t1c.data_sources()[0].parameters["dataset_id"]
        assert t1c_table_name == t1_table_name
        assert t1c_dataset_id != t1_dataset_id
        with closing(storage.connect()) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f'SELECT COUNT(*) FROM "{t1_table_name}";',
                )
                assert cursor.fetchone()[0] == 9
                cursor.execute(
                    f'SELECT COUNT(*) FROM "{t2_table_name}";',
                )
                assert cursor.fetchone()[0] == 3
                cursor.execute(
                    f'SELECT COUNT(*) FROM "{t1c_table_name}" '
                    f"WHERE _dataset_id = {t1c_dataset_id:d};",
                )
                assert cursor.fetchone()[0] == 3

        # Delete all children of b1 (tables t1 and t2), but not b1 itself.
        client["a"]["b1"].delete_contents(
            client["a"]["b1"].keys(), recursive=True, external_only=False
        )
        with closing(storage.connect()) as conn:
            with conn.cursor() as cursor:
                assert sql_table_exists(conn, storage.dialect, t1_table_name)
                cursor.execute(
                    f'SELECT COUNT(*) FROM "{t1_table_name}";',
                )
                assert cursor.fetchone()[0] == 3  # 6 rows deleted
                # Entire t2 deleted
                assert not sql_table_exists(conn, storage.dialect, t2_table_name)

        assert list(client) == ["a"]
        assert list(client["a"]) == ["b1", "b2"]
        assert (
            list(client["a"]["b1"]) == []
        )  # children deleted (2 nodes, 2 data sources, 0 assets)
        assert list(client["a"]["b2"]) == ["table_1_copy"]  # not affected
        nodes_after_delete = (await tree.context.execute("SELECT * from nodes")).all()
        assert len(nodes_after_delete) == 4 + 1  # +1 for the root node
        data_sources_after_delete = (
            await tree.context.execute("SELECT * from data_sources")
        ).all()
        assert len(data_sources_after_delete) == 1
        assets_after_delete = (await tree.context.execute("SELECT * from assets")).all()
        assert len(assets_after_delete) == 1

        # Close and dispose the SQL storage
        storage.dispose()


@pytest.mark.asyncio
async def test_delete_external_asset_registered_twice(tmpdir):
    # Do not use client fixture here.
    # The Context must be opened inside the test or we run into
    # event loop crossing issues with the Postgres test.
    tree = in_memory(readable_storage=[str(tmpdir)])
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)

        for i in range(1, 4):
            with open(tmpdir / f"test_{i}.csv", "w") as file:
                file.write(
                    """a, b, c
                        1, 2, 3
                        4, 5, 6
                    """
                )
        # a has children b1 and b2, which each contain arrays
        a = client.create_container("a")
        b1 = a.create_container("b1")
        await register(b1, tmpdir / "test_1.csv")
        await register(b1, tmpdir / "test_2.csv")
        b2 = a.create_container("b2")
        await register(b2, tmpdir / "test_1.csv")
        await register(b2, tmpdir / "test_3.csv")

        # test_1.csv is registered in both b1 and b2
        assert client["a"]["b1"]["test_1"].read() is not None
        assert client["a"]["b2"]["test_1"].read() is not None

        data_sources_before_delete = (
            await tree.context.execute("SELECT * from data_sources")
        ).all()
        assert len(data_sources_before_delete) == 4
        assets_after_delete = (await tree.context.execute("SELECT * from assets")).all()
        assert len(assets_after_delete) == 3  # shared by two data sources

        a.delete_contents("b2", recursive=True)

        data_sources_after_delete = (
            await tree.context.execute("SELECT * from data_sources")
        ).all()
        assert len(data_sources_after_delete) == 2
        assets_after_delete = (await tree.context.execute("SELECT * from assets")).all()
        assert len(assets_after_delete) == 2

        # The asset in b1 should still be accessible.
        client["a"]["b1"]["test_1"].read()

        a.delete_contents("b1", recursive=True)
        data_sources_after_second_delete = (
            await tree.context.execute("SELECT * from data_sources")
        ).all()
        assert len(data_sources_after_second_delete) == 0
        assets_after_second_delete = (
            await tree.context.execute("SELECT * from assets")
        ).all()
        assert len(assets_after_second_delete) == 0


@pytest.mark.parametrize(
    "assets",
    [
        [
            Asset(
                data_uri="file://localhost/test1",
                is_directory=False,
                parameter="filepath",
                num=None,
            ),
            Asset(
                data_uri="file://localhost/test2",
                is_directory=False,
                parameter="filepath",
                num=1,
            ),
        ],
        [
            Asset(
                data_uri="file://localhost/test1",
                is_directory=False,
                parameter="filepath",
                num=1,
            ),
            Asset(
                data_uri="file://localhost/test2",
                is_directory=False,
                parameter="filepath",
                num=None,
            ),
        ],
        [
            Asset(
                data_uri="file://localhost/test1",
                is_directory=False,
                parameter="filepath",
                num=None,
            ),
            Asset(
                data_uri="file://localhost/test2",
                is_directory=False,
                parameter="filepath",
                num=None,
            ),
        ],
        [
            Asset(
                data_uri="file://localhost/test1",
                is_directory=False,
                parameter="filepath",
                num=1,
            ),
            Asset(
                data_uri="file://localhost/test2",
                is_directory=False,
                parameter="filepath",
                num=1,
            ),
        ],
    ],
    ids=[
        "null-then-int",
        "int-then-null",
        "duplicate-null",
        "duplicate-int",
    ],
)
@pytest.mark.asyncio
async def test_constraints_on_parameter_and_num(a, assets):
    "Test constraints enforced by database on 'parameter' and 'num'."
    arr_adapter = ArrayAdapter.from_array([1, 2, 3])
    with pytest.raises(
        (
            sqlalchemy.exc.IntegrityError,  # SQLite
            sqlalchemy.exc.DBAPIError,  # PostgreSQL
        )
    ):
        await a.create_node(
            key="test",
            structure_family=arr_adapter.structure_family,
            metadata=dict(arr_adapter.metadata()),
            specs=arr_adapter.specs,
            data_sources=[
                DataSource(
                    structure_family=arr_adapter.structure_family,
                    mimetype="text/csv",
                    structure=arr_adapter.structure(),
                    parameters={},
                    management=Management.external,
                    assets=assets,
                )
            ],
        )


@pytest.mark.asyncio
async def test_init_db_logging(sqlite_or_postgres_uri, tmpdir, caplog):
    config = {
        "database": {
            "uri": "sqlite://",  # in-memory
        },
        "trees": [
            {
                "tree": "catalog",
                "path": "/",
                "args": {
                    "uri": sqlite_or_postgres_uri,
                    "writable_storage": str(tmpdir / "data"),
                    "init_if_not_exists": True,
                },
            },
        ],
    }
    # Issue 721 notes that the logging of the subprocess that creates
    # a database logs normal things to error. This test looks at the log
    # and fails if an error log happens. This could catch anything that is
    # an error during the app build.
    import logging

    with caplog.at_level(logging.INFO):
        app = build_app_from_config(config)
        for record in caplog.records:
            assert record.levelname != "ERROR", f"Error found creating app {record.msg}"
        assert app


@pytest.mark.parametrize(
    "exact_count_limit, expected_lower_bound", [(None, 10), (5, 6), (-1, 10)]
)
@pytest.mark.asyncio
async def test_container_length(
    sqlite_or_postgres_uri, exact_count_limit, expected_lower_bound
):
    config = {
        "trees": [
            {
                "tree": "catalog",
                "path": "/",
                "args": {
                    "uri": sqlite_or_postgres_uri,
                    "init_if_not_exists": True,
                },
            },
        ],
    }
    if exact_count_limit is not None:
        config["exact_count_limit"] = exact_count_limit

    app = build_app_from_config(config)

    # Turn off autovacuum in Postgres (just in case)
    # Create a separate engine to avoid interfeing with the running loop
    if sqlite_or_postgres_uri.startswith("postgresql"):
        engine = create_async_engine(
            ensure_specified_sql_driver(sqlite_or_postgres_uri)
        )
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    """
                    ALTER TABLE nodes
                    SET (autovacuum_enabled = false,
                        autovacuum_analyze_threshold = 0);
                """
                )
            )

    with Context.from_app(app) as context:
        client = from_context(context)

        # Create a container with some nested nodes
        a = client.create_container("a")
        for i in range(10):
            b = a.create_container(key=f"node_{i}")
            b.create_container(key=f"subnode_{i}")

        # Before analyzing the table, the length should be thresholded
        len_from_metadata = client["a"].item["attributes"]["structure"]["count"]
        assert len_from_metadata == expected_lower_bound

        # Analyze the table to get update pg_statistics
        if sqlite_or_postgres_uri.startswith("postgresql"):
            async with engine.connect() as conn:
                conn = await conn.execution_options(isolation_level="AUTOCOMMIT")
                await conn.execute(text("VACUUM ANALYZE nodes;"))
            await engine.dispose()

        # After analyzing, the length should be updated (at least be approximate)
        len_from_metadata = client["a"].item["attributes"]["structure"]["count"]
        assert len_from_metadata <= 10

        # len() returns the exact count
        assert len(client["a"]) == 10


@pytest.mark.parametrize(
    "desired, expected",
    [((None, None, None, None), (5, 5, 10, 10)), ((7, 11, 13, 17), (7, 11, 13, 17))],
)
def test_pooling_config(sqlite_or_postgres_uri, sql_storage_uri, desired, expected):
    config = {
        "trees": [
            {
                "tree": "catalog",
                "path": "/",
                "args": {
                    "uri": sqlite_or_postgres_uri,
                    "writable_storage": sql_storage_uri,
                    "init_if_not_exists": True,
                },
            },
        ],
        "catalog_pool_size": desired[0],
        "storage_pool_size": desired[1],
        "catalog_max_overflow": desired[2],
        "storage_max_overflow": desired[3],
    }

    app = build_app_from_config(config)

    # Check the catalog pool
    catalog_pool = app.state.root_tree.context.engine.pool
    assert isinstance(catalog_pool, AsyncAdaptedQueuePool)
    assert catalog_pool.size() == expected[0]
    assert catalog_pool._max_overflow == expected[2]

    # Check the storage pool
    storage = get_storage(ensure_uri(sanitize_uri(sql_storage_uri)[0]))
    storage: SQLStorage = cast(SQLStorage, storage)

    if sql_storage_uri.startswith("duckdb"):
        # DuckDB does not support pooling
        assert isinstance(storage._connection_pool, StaticPool)
        assert storage.pool_size == 1
        assert storage.max_overflow == 0
    else:
        assert isinstance(storage._connection_pool, QueuePool)
        assert storage.pool_size == expected[1]
        assert storage.max_overflow == expected[3]
        assert storage._connection_pool.size() == expected[1]
        assert storage._connection_pool._max_overflow == expected[3]

    storage.dispose()
