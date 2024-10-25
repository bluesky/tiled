import random
import string
from dataclasses import asdict

import dask.array
import numpy
import pandas
import pandas.testing
import pytest
import pytest_asyncio
import sqlalchemy.dialects.postgresql.asyncpg
import sqlalchemy.exc
import tifffile
import xarray

from ..adapters.csv import read_csv
from ..adapters.dataframe import ArrayAdapter
from ..adapters.tiff import TiffAdapter
from ..catalog import in_memory
from ..catalog.adapter import WouldDeleteData
from ..catalog.explain import record_explanations
from ..client import Context, from_context
from ..client.xarray import write_xarray_dataset
from ..queries import Eq, Key
from ..server.app import build_app, build_app_from_config
from ..server.schemas import Asset, DataSource, Management
from ..structures.core import StructureFamily
from ..utils import ensure_uri
from .utils import enter_username_password


@pytest_asyncio.fixture
async def a(adapter):
    "Raw adapter, not to be used within an app becaues it is manually started and stopped."
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
    assert b.segments == ["b"]
    assert c.segments == ["b", "c"]
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
    await a.sort([("id", 1)]).keys_range(0, 10) == ordered_letters
    # Test again, with items_range.
    [k for k, v in await a.sort([("id", 1)]).items_range(0, 10)] == ordered_letters

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
    assert await a.search(Eq("letter", "c")).lookup_adapter(["d"]) is None

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
    dfa = read_csv(data_uri)
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
    arr = numpy.ones((5, 3))
    ad = ArrayAdapter.from_array(arr)
    structure = asdict(ad.structure())
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
    await x.write(arr)
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
        {"temp": (["time"], [101, 102, 103])},
        coords={"time": (["time"], [1, 2, 3])},
    )
    dsc = write_xarray_dataset(client, ds, key="test_xarray_dataset")
    assert set(dsc) == {"temp", "time"}
    # smoke test
    dsc["temp"][:]
    dsc["time"][:]
    dsc.read()


@pytest.mark.asyncio
async def test_delete_tree(tmpdir):
    # Do not use client fixture here.
    # The Context must be opened inside the test or we run into
    # event loop crossing issues with the Postgres test.
    tree = in_memory(writable_storage=tmpdir)
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
        assert len(nodes_before_delete) == 7
        data_sources_before_delete = (
            await tree.context.execute("SELECT * from data_sources")
        ).all()
        assert len(data_sources_before_delete) == 3
        assets_before_delete = (
            await tree.context.execute("SELECT * from assets")
        ).all()
        assert len(assets_before_delete) == 3

        with pytest.raises(WouldDeleteData):
            await tree.delete_tree()  # external_only=True by default
        with pytest.raises(WouldDeleteData):
            await tree.delete_tree(external_only=True)
        await tree.delete_tree(external_only=False)

        nodes_after_delete = (await tree.context.execute("SELECT * from nodes")).all()
        assert len(nodes_after_delete) == 0
        data_sources_after_delete = (
            await tree.context.execute("SELECT * from data_sources")
        ).all()
        assert len(data_sources_after_delete) == 0
        assets_after_delete = (await tree.context.execute("SELECT * from assets")).all()
        assert len(assets_after_delete) == 0


@pytest.mark.asyncio
async def test_access_control(tmpdir):
    config = {
        "authentication": {
            "allow_anonymous_access": True,
            "secret_keys": ["SECRET"],
            "providers": [
                {
                    "provider": "toy",
                    "authenticator": "tiled.authenticators:DictionaryAuthenticator",
                    "args": {
                        "users_to_passwords": {
                            "alice": "secret1",
                            "bob": "secret2",
                            "admin": "admin",
                        }
                    },
                }
            ],
        },
        "database": {
            "uri": "sqlite+aiosqlite://",  # in-memory
        },
        "trees": [
            {
                "tree": "catalog",
                "path": "/",
                "args": {
                    "uri": f"sqlite+aiosqlite:///{tmpdir}/catalog.db",
                    "writable_storage": str(tmpdir / "data"),
                    "init_if_not_exists": True,
                },
                "access_control": {
                    "access_policy": "tiled.access_policies:SimpleAccessPolicy",
                    "args": {
                        "provider": "toy",
                        "access_lists": {
                            "alice": ["outer_x"],
                            "bob": ["outer_y"],
                        },
                        "admins": ["admin"],
                        "public": ["outer_z"],
                    },
                },
            },
        ],
    }

    app = build_app_from_config(config)
    with Context.from_app(app) as context:
        with enter_username_password("admin", "admin"):
            admin_client = from_context(context, username="admin")
            for key in ["outer_x", "outer_y", "outer_z"]:
                container = admin_client.create_container(key)
                container.write_array([1, 2, 3], key="inner")
            admin_client.logout()
        with enter_username_password("alice", "secret1"):
            alice_client = from_context(context, username="alice")
            alice_client["outer_x"]["inner"].read()
            with pytest.raises(KeyError):
                alice_client["outer_y"]
            alice_client.logout()
        public_client = from_context(context)
        public_client["outer_z"]["inner"].read()
        with pytest.raises(KeyError):
            public_client["outer_x"]


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
                    structure=asdict(arr_adapter.structure()),
                    parameters={},
                    management=Management.external,
                    assets=assets,
                )
            ],
        )
