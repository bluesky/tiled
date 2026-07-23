import copy
import subprocess
import sys
from typing import Any, Dict, Union

import httpx
import numpy
import pytest
from httpx import ASGITransport, AsyncClient
from respx import MockRouter
from starlette.status import HTTP_200_OK, HTTP_401_UNAUTHORIZED

from tests.conftest import TOY_AUTHENTICATION
from tests.utils import fail_with_status_code
from tiled.adapters.array import ArrayAdapter
from tiled.adapters.mapping import MapAdapter
from tiled.client.context import Context
from tiled.server.app import build_app, build_app_from_config

arr = ArrayAdapter.from_array(numpy.ones((5, 5)))
tree = MapAdapter({"A1": arr, "A2": arr})


@pytest.fixture
def well_known_url() -> str:
    return "http://example.com/well_known/"


@pytest.fixture
def config(sqlite_or_postgres_uri):
    """
    Return config with
    - a unique temporary sqlite database location
    - a unique nested dict instance that the test can mutate
    """
    database_uri = sqlite_or_postgres_uri
    subprocess.run(
        [sys.executable, "-m", "tiled", "admin", "initialize-database", database_uri],
        check=True,
        capture_output=True,
    )
    return {
        "authentication": copy.deepcopy(TOY_AUTHENTICATION),
        "database": {
            "uri": database_uri,
        },
        "trees": [
            {
                "tree": f"{__name__}:tree",
                "path": "/",
            },
        ],
    }


@pytest.fixture
def tokens_response() -> Dict[str, str]:
    return {
        "access_token": "jwt",
        "expires_in": "60",
        "refresh_expires_in": "0",
        "refresh_token": "jwt",
        "token_type": "Bearer",
        "id_token": "jwt",
        "not-before-policy": "0",
        "session_state": "uuid",
        "scope": "offline_access email",
    }


@pytest.fixture
def oidc_config(well_known_url: str) -> Dict[str, Any]:
    return {
        "authentication": {
            "secret_keys": ["SECRET"],
            "providers": [
                {
                    "provider": "keycloak_oidc",
                    "authenticator": "tiled.authenticators:ProxiedOIDCAuthenticator",
                    "args": {
                        "audience": "tiled_aud",
                        "client_id": "tiled",
                        "device_flow_client_id": "tiled-cli",
                        "well_known_uri": well_known_url,
                        "confirmation_message": "You have logged in with Proxied OIDC as {id}.",
                        "scopes": ["read:data"],
                    },
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
def mock_oidc_server(
    respx_mock: MockRouter,
    base_url: str,
    well_known_url: str,
    well_known_response: Dict[str, Any],
    tokens_response: Dict[str, Union[str, int]],
    oidc_config: Dict[str, Any],
) -> MockRouter:
    respx_mock.get(well_known_url).mock(
        return_value=httpx.Response(httpx.codes.OK, json=well_known_response)
    )

    device_flow_client_id = oidc_config["authentication"]["providers"][0]["args"][
        "device_flow_client_id"
    ]
    device_code = "FsYWEv-Fl4wkFlrtp-EWH7HR1pkCG2NIfBNeUKlZBAY"
    user_code = "LCWE-ROXW"
    verification_uri = f"{base_url}device"
    verification_uri_complete = f"{base_url}device?user_code={user_code}"

    respx_mock.post(
        well_known_response["device_authorization_endpoint"],
        data={"client_id": device_flow_client_id, "scope": "offline_access openid"},
    ).mock(
        return_value=httpx.Response(
            status_code=httpx.codes.OK,
            json={
                "device_code": device_code,
                "user_code": user_code,
                "verification_uri": verification_uri,
                "verification_uri_complete": verification_uri_complete,
                "expires_in": 600,
                "interval": 5,
            },
        )
    )

    respx_mock.post(
        well_known_response["token_endpoint"],
        data={
            "device_code": device_code,
            "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
            "client_id": device_flow_client_id,
        },
        name="token_polling",
    ).mock(
        return_value=httpx.Response(
            status_code=httpx.codes.OK,
            json=tokens_response,
        )
    )
    return respx_mock


@pytest.mark.parametrize("path", ["/", "/docs", "/healthz"])
@pytest.mark.asyncio
async def test_meta_routes(path):
    transport = ASGITransport(app=build_app({}))
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(path)
    assert response.status_code == HTTP_200_OK


def test_oidc(enter_username_password, config, oidc_config, mock_oidc_server):
    with Context.from_app(build_app_from_config(config)) as context:
        with enter_username_password("alice", "secret1"):
            context.authenticate()

        api_key = context.create_api_key(scopes=["read:data"])

    with Context.from_app(
        build_app_from_config(oidc_config), api_key=api_key["secret"]
    ) as test_proxied_context:
        with fail_with_status_code(HTTP_401_UNAUTHORIZED):
            test_proxied_context.create_api_key()
