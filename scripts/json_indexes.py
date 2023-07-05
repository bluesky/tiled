import asyncio

from tiled._tests.test_catalog import temp_postgres
from tiled.catalog.adapter import Adapter
from tiled.catalog.explain import record_explanations
from tiled.queries import Key


async def test(a):
    for i in range(100):
        await a.create_container(
            metadata={
                "number": i,
                "number_as_string": str(i),
                "nested": {"number": i, "number_as_string": str(i)},
            },
            specs=[],
            structure_family="array",
        )
    # await a.create_metadata_index("nested_number_as_string", "nested.number_as_string")
    # await a.create_metadata_index("nested_number", "nested.number")
    with record_explanations() as e:
        results = await a.search(Key("number_as_string") == "3").keys_slice(0, 5, 1)
        assert len(results) == 1
        results = await a.search(Key("number") == 3).keys_slice(0, 5, 1)
        assert len(results) == 1
        results = await a.search(Key("nested.number_as_string") == "3").keys_slice(
            0, 5, 1
        )
        assert len(results) == 1
        results = await a.search(Key("nested.number") == 3).keys_slice(0, 5, 1)
        assert len(results) == 1
    print(e)


async def test_sqlite():
    async with Adapter.async_in_memory(echo=True) as a:
        await test(a)


async def test_postgres():
    async with temp_postgres(
        "postgresql+asyncpg://postgres:secret@localhost:5432", echo=True
    ) as a:
        await test(a)


# asyncio.run(test_postgres())
asyncio.run(test_sqlite())
