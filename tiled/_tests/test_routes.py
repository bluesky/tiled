import pytest
from httpx import ASGITransport, AsyncClient
from starlette.status import HTTP_200_OK

from ..server.app import build_app


@pytest.mark.parametrize("path", ["/", "/docs", "/healthz"])
@pytest.mark.asyncio
async def test_meta_routes(path):
    transport = ASGITransport(app=build_app({}))
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(path)
    assert response.status_code == HTTP_200_OK
