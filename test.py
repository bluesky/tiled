import asyncio
from tiled.structures.core import StructureFamily
from tiled.catalog.adapter import Adapter
from tiled.catalog.explain import record_explanations
from tiled.queries import Key
from tiled._tests.test_catalog import temp_postgres


async def test_sqlite():
    async with Adapter.async_in_memory(echo=True) as a:
        # for i in range(10):
        #     await a.create_node(metadata={"number": i, "number_as_string": str(i)}, specs=[], references=[], structure_family="array")
        with record_explanations() as e:
            results = await a.search(Key("number") == 3).keys_slice(0, 5, 1)
            print("RESULTS", results)
            assert len(results) == 1
        print(e)

async def test_postgres():
    async with temp_postgres("postgresql+asyncpg://postgres:secret@localhost:5432", echo=True) as a:
        # for i in range(10):
        #     await a.create_node(metadata={"number": i, "number_as_string": str(i)}, specs=[], references=[], structure_family="array")
        with record_explanations() as e:
            results = await a.search(Key("number") == 3).keys_slice(0, 5, 1)
            print("RESULTS", results)
            # assert len(results) == 1
        print(e)

asyncio.run(test_postgres())
# asyncio.run(test_sqlite())
