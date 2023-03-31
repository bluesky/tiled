import asyncio

from tiled._tests.test_catalog import temp_postgres
from tiled.catalog.node import async_in_memory
from tiled.client import Context, from_context
from tiled.queries import Key
from tiled.server.app import build_app


async def test(a):
    app = build_app(a)
    with Context.from_app(app) as context:
        client = from_context(context)
        print(list(client))
        client.write_array([1, 2, 3])


async def test_sqlite():
    async with async_in_memory(writable_storage="file:///tmp/data") as a:
        await test(a)


asyncio.run(test_sqlite())
