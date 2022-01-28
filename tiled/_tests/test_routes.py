import pytest
from httpx import AsyncClient

from ..server.app import build_app


@pytest.mark.parametrize("path", ["/", "/docs"])
@pytest.mark.asyncio
async def test_meta_routes(path):
    app = build_app({})
    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.get(path)
    assert response.status_code == 200
