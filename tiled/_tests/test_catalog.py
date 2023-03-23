import random
import string

import pytest

from ..catalog.adapter import Adapter
from ..queries import Eq
from ..structures.core import StructureFamily


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
async def test_nested_node_creation():
    a = await Adapter.async_in_memory()
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
async def test_sorting():
    a = await Adapter.async_in_memory()
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
async def test_search():
    a = await Adapter.async_in_memory()
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
