import os
import random
import string
import uuid

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from ..catalog.adapter import Adapter
from ..queries import Eq
from ..structures.core import StructureFamily

# To test with postgres, start a container like:
#
# docker run --name tiled-test-postgres -p 5432:5432 -e POSTGRES_PASSWORD=secret -d docker.io/postgres
# and set this env var like:
#
# TILED_TEST_POSTGRESQL_URI=postgresql+asyncpg://postgres:secret@localhost:5432

TILED_TEST_POSTGRESQL_URI = os.getenv("TILED_TEST_POSTGRESQL_URI")
if TILED_TEST_POSTGRESQL_URI and TILED_TEST_POSTGRESQL_URI.endswith("/"):
    TILED_TEST_POSTGRESQL_URI = TILED_TEST_POSTGRESQL_URI[:-1]


@pytest_asyncio.fixture(params=["sqlite", "postgresql"])
async def a(request):
    "Adapter instance"
    if request.param == "sqlite":
        async with Adapter.async_in_memory() as adapter:
            yield adapter
    elif request.param == "postgresql":
        if not TILED_TEST_POSTGRESQL_URI:
            raise pytest.skip("No TILED_TEST_POSTGRESQL_URI configured")
        # Create a fresh database.
        engine = create_async_engine(TILED_TEST_POSTGRESQL_URI)
        database_name = f"tiled_test_disposable_{uuid.uuid4().hex}"
        async with engine.connect() as connection:
            await connection.execute(
                text("COMMIT")
            )  # close the automatically-started transaction
            await connection.execute(text(f"CREATE DATABASE {database_name};"))
            await connection.commit()
        # Use the database.
        async with Adapter.async_create_from_uri(
            f"{TILED_TEST_POSTGRESQL_URI}/{database_name}"
        ) as adapter:
            yield adapter
        # Drop the database.
        async with engine.connect() as connection:
            await connection.execute(
                text("COMMIT")
            )  # close the automatically-started transaction
            await connection.execute(text(f"DROP DATABASE {database_name};"))
            await connection.commit()
    else:
        assert False


def test_constructors(tmpdir):
    # Create an adapter with a database in memory.
    Adapter.in_memory()
    # Cannot connect to database that does not exist.
    # with pytest.raises(DatabaseNotFound):
    #     Adapter.from_uri(f"sqlite+aiosqlite:///{tmpdir}/database.sqlite")
    # Create one.
    Adapter.create_from_uri(f"sqlite+aiosqlite:///{tmpdir}/database.sqlite")
    # Now connecting works.
    Adapter.from_uri(f"sqlite+aiosqlite:///{tmpdir}/database.sqlite")


@pytest.mark.asyncio
async def test_nested_node_creation(a):
    b = await a.create_node(
        key="b",
        metadata={},
        structure_family=StructureFamily.array,
        specs=[],
        references=[],
    )
    c = await b.create_node(
        key="c",
        metadata={},
        structure_family=StructureFamily.array,
        specs=[],
        references=[],
    )
    assert b.segments == ["b"]
    assert c.segments == ["b", "c"]
    assert (await a.keys_slice(0, 1, 1)) == ["b"]
    assert (await b.keys_slice(0, 1, 1)) == ["c"]
    # smoke test
    await a.items_slice(0, 1, 1)
    await b.items_slice(0, 1, 1)


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
            structure_family=StructureFamily.array,
            specs=[],
            references=[],
        )
    # Default order is based on time_created.
    assert (await a.keys_slice(0, 10, 1)) == shuffled_letters

    # Explicitly sort by time_created.
    assert (
        await a.sort([("time_created", 1)]).keys_slice(0, 10, 1)
    ) == shuffled_letters

    # Sort by key.
    await a.sort([("id", 1)]).keys_slice(0, 10, 1) == ordered_letters
    # Test again, with items_slice.
    [k for k, v in await a.sort([("id", 1)]).items_slice(0, 10, 1)] == ordered_letters

    # Sort by letter metadata.
    # Use explicit 'metadata.{key}' namespace.
    assert (
        await a.sort([("metadata.letter", 1)]).keys_slice(0, 10, 1)
    ) == ordered_letters

    # Sort by letter metadata.
    # Use implicit '{key}' (more convenient, and necessary for back-compat).
    assert (await a.sort([("letter", 1)]).keys_slice(0, 10, 1)) == ordered_letters

    # Sort by number and then by letter.
    # Use explicit 'metadata.{key}' namespace.
    items = await a.sort([("metadata.number", 1), ("metadata.letter", 1)]).items_slice(
        0, 10, 1
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
            structure_family=StructureFamily.array,
            specs=[],
            references=[],
        )
    assert "c" in await a.keys_slice(0, 5, 1)
    assert await a.search(Eq("letter", "c")).keys_slice(0, 5, 1) == ["c"]
    assert await a.search(Eq("number", 2)).keys_slice(0, 5, 1) == ["c"]

    # Looking up "d" inside search results should find nothing when
    # "d" is filtered out by a search query first.
    assert await a.lookup(["d"]) is not None
    assert await a.search(Eq("letter", "c")).lookup(["d"]) is None

    # Search on nested key.
    assert await a.search(Eq("x.y.z", "c")).keys_slice(0, 5, 1) == ["c"]

    # Created nested nodes and search on them.
    d = await a.lookup(["d"])
    for letter, number in zip(string.ascii_lowercase[:5], range(10, 15)):
        await d.create_node(
            key=letter,
            metadata={"letter": letter, "number": number},
            structure_family=StructureFamily.array,
            specs=[],
            references=[],
        )
    assert await d.search(Eq("letter", "c")).keys_slice(0, 5, 1) == ["c"]
    assert await d.search(Eq("number", 12)).keys_slice(0, 5, 1) == ["c"]


@pytest.mark.asyncio
async def test_metadata_indexes(a):
    # There is some weird coupling happening with pytest
    # that needs investiagtion. In the mean time, lead with this:
    await a.drop_all_metadata_indexes()

    # Test create / list / drop.
    assert len(await a.list_metadata_indexes()) == 0
    await a.create_metadata_index("letter", "letter")
    indexes = await a.list_metadata_indexes()
    assert len(indexes) == 1
    assert indexes[0][0] == "tiled_md_letter"
    await a.drop_metadata_index("tiled_md_letter")
    assert len(await a.list_metadata_indexes()) == 0
    await a.drop_all_metadata_indexes()
