import contextlib
import os
import random
import string
import uuid
from dataclasses import asdict

import numpy
import pandas
import pandas.testing
import pytest
import pytest_asyncio
import tifffile
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from ..adapters.dataframe import ArrayAdapter, DataFrameAdapter
from ..adapters.tiff import TiffAdapter
from ..catalog.explain import record_explanations
from ..catalog.node import (
    async_create_from_uri,
    async_in_memory,
    create_from_uri,
    from_uri,
    in_memory,
)
from ..client import Context, from_context
from ..queries import Eq, Key
from ..server.app import build_app
from ..server.schemas import Asset, DataSource
from ..structures.array import ArrayStructure
from ..structures.core import StructureFamily
from ..structures.dataframe import DataFrameStructure

# To test with postgres, start a container like:
#
# docker run --name tiled-test-postgres -p 5432:5432 -e POSTGRES_PASSWORD=secret -d docker.io/postgres
# and set this env var like:
#
# TILED_TEST_POSTGRESQL_URI=postgresql+asyncpg://postgres:secret@localhost:5432

TILED_TEST_POSTGRESQL_URI = os.getenv("TILED_TEST_POSTGRESQL_URI")


@contextlib.asynccontextmanager
async def temp_postgres(uri, *args, **kwargs):
    if uri.endswith("/"):
        uri = uri[:-1]
    # Create a fresh database.
    engine = create_async_engine(uri)
    database_name = f"tiled_test_disposable_{uuid.uuid4().hex}"
    async with engine.connect() as connection:
        await connection.execute(
            text("COMMIT")
        )  # close the automatically-started transaction
        await connection.execute(text(f"CREATE DATABASE {database_name};"))
        await connection.commit()
    # Use the database.
    async with async_create_from_uri(
        f"{uri}/{database_name}",
        *args,
        **kwargs,
    ) as adapter:
        yield adapter
    # Drop the database.
    async with engine.connect() as connection:
        await connection.execute(
            text("COMMIT")
        )  # close the automatically-started transaction
        await connection.execute(text(f"DROP DATABASE {database_name};"))
        await connection.commit()


@pytest_asyncio.fixture(params=["sqlite", "postgresql"])
async def a(request, tmpdir):
    "Adapter instance"
    if request.param == "sqlite":
        async with async_in_memory(writable_storage=str(tmpdir)) as adapter:
            yield adapter
    elif request.param == "postgresql":
        if not TILED_TEST_POSTGRESQL_URI:
            raise pytest.skip("No TILED_TEST_POSTGRESQL_URI configured")
        async with temp_postgres(
            TILED_TEST_POSTGRESQL_URI, writable_storage=str(tmpdir)
        ) as adapter:
            yield adapter
    else:
        assert False


def test_constructors(tmpdir):
    # Create an adapter with a database in memory.
    in_memory()
    # Cannot connect to database that does not exist.
    # with pytest.raises(DatabaseNotFound):
    #     Adapter.from_uri(f"sqlite+aiosqlite:///{tmpdir}/database.sqlite")
    # Create one.
    create_from_uri(f"sqlite+aiosqlite:///{tmpdir}/database.sqlite")
    # Now connecting works.
    from_uri(f"sqlite+aiosqlite:///{tmpdir}/database.sqlite")


@pytest.mark.asyncio
async def test_nested_node_creation(a):
    await a.create_node(
        key="b",
        metadata={},
        structure_family=StructureFamily.node,
        specs=[],
        references=[],
    )
    b = await a.lookup_adapter(["b"])
    await b.create_node(
        key="c",
        metadata={},
        structure_family=StructureFamily.node,
        specs=[],
        references=[],
    )
    c = await b.lookup_adapter(["c"])
    assert b.segments == ["b"]
    assert c.segments == ["b", "c"]
    assert (await a.keys_range(0, 1)) == ["b"]
    assert (await b.keys_range(0, 1)) == ["c"]
    # smoke test
    await a.items_range(0, 1)
    await b.items_range(0, 1)


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
            structure_family=StructureFamily.node,
            specs=[],
            references=[],
        )
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
    numbers = [v.metadata["number"] for k, v in items]
    letters = [v.metadata["letter"] for k, v in items]
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
            structure_family=StructureFamily.node,
            specs=[],
            references=[],
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
            structure_family=StructureFamily.node,
            specs=[],
            references=[],
        )
    assert await d.search(Eq("letter", "c")).keys_range(0, 5) == ["c"]
    assert await d.search(Eq("number", 12)).keys_range(0, 5) == ["c"]


@pytest.mark.asyncio
async def test_metadata_index_is_used(a):
    for i in range(10):
        await a.create_node(
            metadata={
                "number": i,
                "number_as_string": str(i),
                "nested": {"number": i, "number_as_string": str(i), "bool": bool(i)},
                "bool": bool(i),
            },
            specs=[],
            references=[],
            structure_family="array",
        )
    # Check that an index (specifically the 'top_level_metdata' index) is used
    # by inspecting the content of an 'EXPLAIN ...' query. The exact content
    # is intended for humans and is not an API, but we can coarsely check
    # that the index of interest is mentioned.
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


@pytest.mark.asyncio
async def test_write_array_external(a, tmpdir):
    arr = numpy.ones((5, 3))
    filepath = tmpdir / "file.tiff"
    tifffile.imwrite(str(filepath), arr)
    ad = TiffAdapter(str(filepath))
    structure = asdict(
        ArrayStructure(macro=ad.macrostructure(), micro=ad.microstructure())
    )
    await a.create_node(
        key="x",
        structure_family="array",
        metadata={},
        data_sources=[
            DataSource(
                mimetype="image/tiff",
                structure=structure,
                parameters={},
                management="external",
                assets=[Asset(data_uri=f"file:///{filepath}", is_directory=False)],
            )
        ],
    )
    x = await a.lookup_adapter(["x"])
    assert numpy.array_equal(arr, x.read())


@pytest.mark.asyncio
async def test_write_dataframe_external(a, tmpdir):
    df = pandas.DataFrame(numpy.ones((5, 3)), columns=list("abc"))
    filepath = tmpdir / "file.csv"
    df.to_csv(filepath, index=False)
    dfa = DataFrameAdapter.read_csv(filepath)
    structure = asdict(
        DataFrameStructure(macro=dfa.macrostructure(), micro=dfa.microstructure())
    )
    await a.create_node(
        key="x",
        structure_family="dataframe",
        metadata={},
        data_sources=[
            DataSource(
                mimetype="text/csv",
                structure=structure,
                parameters={},
                management="external",
                assets=[Asset(data_uri=f"file:///{filepath}", is_directory=False)],
            )
        ],
    )
    x = await a.lookup_adapter(["x"])
    pandas.testing.assert_frame_equal(df, x.read())


@pytest.mark.asyncio
async def test_write_array_internal(a, tmpdir):
    arr = numpy.ones((5, 3))
    ad = ArrayAdapter(arr)
    structure = asdict(
        ArrayStructure(macro=ad.macrostructure(), micro=ad.microstructure())
    )
    await a.create_node(
        key="x",
        structure_family="array",
        metadata={},
        data_sources=[
            DataSource(
                structure=structure,
                management="writable",
            )
        ],
    )
    x = await a.lookup_adapter(["x"])
    await x.write(arr)
    assert numpy.array_equal(arr, x.read())


def test_server(a):
    app = build_app(a)
    with Context.from_app(app) as context:
        client = from_context(context)
        list(client)
        client.write_array([1, 2, 3])
