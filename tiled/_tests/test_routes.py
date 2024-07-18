import pytest
from httpx import AsyncClient
from starlette.status import HTTP_200_OK

from ..server.app import build_app


@pytest.mark.parametrize("path", ["/", "/docs", "/healthz"])
@pytest.mark.asyncio
async def test_meta_routes(path):
    app = build_app({})
    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.get(path)
    assert response.status_code == HTTP_200_OK
