import json

import pytest
from starlette.status import HTTP_200_OK

from tiled.adapters.mapping import MapAdapter
from tiled.client import Context
from tiled.server.app import build_app_from_config

# Basic authenticated server config
tree = MapAdapter({})
config = {
    "authentication": {
        "secret_keys": ["SECRET"],
        "providers": [
            {
                "provider": "toy",
                "authenticator": "tiled.authenticators:DictionaryAuthenticator",
                "args": {"users_to_passwords": {"alice": "secret1", "bob": "secret2"}},
            }
        ],
    },
    "trees": [
        {
            "tree": f"{__name__}:tree",
            "path": "/",
        },
    ],
}


@pytest.fixture
def context():
    with Context.from_app(build_app_from_config(config)) as context:
        yield context


def test_openapi_username_password_login(context):
    """
    Ensure that tokenUrl is a valid path.

    The tokenUrl is set manually because of when in the server lifecycle it is
    known, and the point of this test is to ensure it does not bit rot to some
    invalid path, because that has happened before.
    """
    response = context.http_client.get("/openapi.json")
    assert response.status_code == HTTP_200_OK
    openapi = response.json()
    token_url = openapi["components"]["securitySchemes"]["OAuth2PasswordBearer"][
        "flows"
    ]["password"]["tokenUrl"]
    assert token_url in openapi["paths"]


def test_openapi_default_schema(context):
    """Default /openapi.json returns the full schema with components and all paths."""
    response = context.http_client.get("/openapi.json")
    assert response.status_code == HTTP_200_OK
    spec = response.json()
    assert "components" in spec
    # Should include POST/PUT endpoints, not just GET
    methods_seen = set()
    for path_item in spec["paths"].values():
        methods_seen.update(path_item.keys())
    assert "post" in methods_seen or "put" in methods_seen


def test_openapi_agent_schema(context):
    """The ?agent variant returns a simplified, self-contained OpenAPI spec."""
    response = context.http_client.get("/openapi.json", params={"agent": ""})
    assert response.status_code == HTTP_200_OK
    spec = response.json()

    # Must be valid OpenAPI with required top-level fields
    assert spec["openapi"].startswith("3.")
    assert spec["info"]["title"] == "Tiled"

    # Must be fully dereferenced — no $ref anywhere and no components section
    raw = json.dumps(spec)
    assert '"$ref"' not in raw, "Agent schema must not contain $ref pointers"
    assert "components" not in spec

    # Only GET endpoints should be exposed
    for path, path_item in spec["paths"].items():
        for method in path_item:
            assert method == "get", f"Non-GET method '{method}' found at {path}"

    # Key endpoints must be present
    assert "/api/v1/search/{path}" in spec["paths"]
    assert "/api/v1/metadata/{path}" in spec["paths"]
    assert "/healthz" in spec["paths"]

    # No 422 responses (stripped for agents)
    for path, path_item in spec["paths"].items():
        for method, op in path_item.items():
            if isinstance(op, dict):
                assert "422" not in op.get(
                    "responses", {}
                ), f"422 response found at {method.upper()} {path}"

    # No per-operation security (agents inject auth externally)
    for path, path_item in spec["paths"].items():
        for method, op in path_item.items():
            if isinstance(op, dict):
                assert (
                    "security" not in op
                ), f"security found at {method.upper()} {path}"
