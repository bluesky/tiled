import pytest
from starlette.status import HTTP_200_OK

from ..adapters.mapping import MapAdapter
from ..client import Context
from ..server.app import build_app_from_config

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
