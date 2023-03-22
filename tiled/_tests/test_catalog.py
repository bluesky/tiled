import pytest

from ..catalog.adapter import Adapter
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
