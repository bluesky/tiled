import pytest
from httpx2 import ASGITransport, AsyncClient
from starlette.status import HTTP_200_OK

from tiled.server.app import build_app


@pytest.mark.parametrize("path", ["/", "/docs", "/healthz"])
@pytest.mark.asyncio
async def test_meta_routes(path):
    transport = ASGITransport(app=build_app({}))
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(path)
    assert response.status_code == HTTP_200_OK


# Routes that serve data or metadata, by prefix, and the ones among them that
# are meant to be open to everyone.
DATA_ROUTE_PREFIXES = ("/api/v1/", "/zarr/")
OPEN_DATA_ROUTES = {"/api/v1/"}


def test_data_routes_require_scopes():
    """Every route that serves data or metadata declares the scopes it requires.

    Scopes are enforced per route. A route without the check is open to any
    request, including one with no credentials.
    """
    paths = build_app({}).openapi()["paths"]
    checked = []
    unguarded = []
    for path, operations in paths.items():
        if not path.startswith(DATA_ROUTE_PREFIXES) or path in OPEN_DATA_ROUTES:
            continue
        for method, operation in operations.items():
            scopes = {
                scope
                for requirement in operation.get("security", [])
                for scheme_scopes in requirement.values()
                for scope in scheme_scopes
            }
            checked.append(path)
            if not scopes:
                unguarded.append(f"{method.upper()} {path}")
    # Guard against this test passing because it looked at nothing.
    assert any(path.startswith("/api/v1/") for path in checked)
    assert any(path.startswith("/zarr/") for path in checked)
    assert not unguarded
