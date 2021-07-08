import pytest
from httpx import AsyncClient

from ..server.app import serve_tree


@pytest.mark.parametrize("path", ["/", "/docs"])
@pytest.mark.asyncio
async def test_meta_routes(path):
    app = serve_tree({})
    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.get(path)
    assert response.status_code == 200
