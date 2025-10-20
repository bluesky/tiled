import textwrap
from pathlib import Path
from typing import Any, Dict, Generator, List, Union
from unittest.mock import MagicMock, patch
from uuid import UUID

import httpx
import pytest
import respx
import stamina
from respx import MockRouter

from tiled.adapters.mapping import MapAdapter
from tiled.client import Context
from tiled.client.auth import TiledAuth
from tiled.client.constructors import from_context
from tiled.client.context import prompt_for_credentials
from tiled.server.schemas import Principal, PrincipalType

from ..server.app import build_app_from_config

tree = MapAdapter({})


@pytest.fixture
def well_known_url() -> str:
    return "http://example.com/well_known/"


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
        data={"client_id": device_flow_client_id, "scope": "openid offline_access"},
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


@pytest.fixture
def context(
    mock_oidc_server: MockRouter,
    oidc_config: Dict[str, Any],
) -> Generator[Context, Any, Any]:
    with Context.from_app(build_app_from_config(oidc_config)) as context:
        yield context


def test_about_endpoint(
    context: Context,
    well_known_response: Dict[str, Union[str, List[str]]],
    oidc_config: Dict[str, Any],
):
    response = context.http_client.get("/api/v1/")
    assert response.status_code == httpx.codes.OK
    assert response.json()["authentication"]["providers"][0]["links"] == {
        "auth_endpoint": well_known_response["device_authorization_endpoint"],
        "client_id": oidc_config["authentication"]["providers"][0]["args"][
            "device_flow_client_id"
        ],
        "token_endpoint": well_known_response["token_endpoint"],
    }


@patch("tiled.client.context.time.sleep")
def test_device_flow_success(
    mock_oidc_server: MockRouter,
    context: Context,
    capsys,
    base_url: str,
    tokens_response: Dict[str, Union[str, int]],
):
    with patch("webbrowser.open", return_value=False):
        tokens = prompt_for_credentials(
            httpx.Client(), context.server_info.authentication.providers
        )

    out, err = capsys.readouterr()
    assert out == textwrap.dedent(
        f"""
        You have 10 minutes to visit this URL

        {base_url}device?user_code=LCWE-ROXW

        and enter the code:

        LCWE-ROXW


        Waiting...
        You have logged in with Proxied OIDC as external user.
    """
    )
    assert err == ""

    assert tokens == tokens_response


@pytest.mark.xfail(reason="This should not fail,but needs investigation in stamina")
@patch("tiled.client.context.time.sleep")
def test_device_flow_polling(
    _: MagicMock,
    mock_oidc_server: MockRouter,
    context: Context,
    capsys,
    base_url: str,
    tokens_response: Dict[str, Union[str, int]],
    well_known_response: Dict[str, Any],
):
    token_polling_route = mock_oidc_server["token_polling"]
    token_polling_route.return_value = None
    token_polling_route.side_effect = [
        httpx.Response(
            status_code=httpx.codes.BAD_REQUEST, json={"error": "authorization_pending"}
        ),
        httpx.Response(
            status_code=httpx.codes.OK,
            json=tokens_response,
        ),
    ]

    stamina.set_testing(testing=True, attempts=1)

    with patch("webbrowser.open", return_value=False):
        tokens = prompt_for_credentials(
            httpx.Client(), context.server_info.authentication.providers
        )

    out, err = capsys.readouterr()
    assert out == textwrap.dedent(
        f"""
        You have 10 minutes to visit this URL

        {base_url}device?user_code=LCWE-ROXW

        and enter the code:

        LCWE-ROXW


        Waiting...
        You have logged in with Proxied OIDC as external user.
    """
    )
    assert err == ""

    assert tokens == tokens_response


@pytest.fixture
def decoded_token(base_url: str) -> Dict[str, Any]:
    return {
        "exp": 1760638732,
        "iat": 1760638672,
        "jti": "onrtna:4615cb85-56c2-a09e-438c-55d79dc7089b",
        "iss": base_url,
        "aud": ["tiled_aud", "master-realm", "account"],
        "sub": "658d8ed5-4993-4e15-bd6a-6aa650144576",
        "typ": "Bearer",
        "azp": "tiled",
        "sid": "7ff39436-5b88-4ff6-ab68-53eb563593c1",
        "scope": "openid email profile read:metadata",
        "email_verified": False,
    }


@pytest.fixture
@patch("tiled.authenticators.OIDCAuthenticator.decode_token")
def client(
    decode_token: MagicMock,
    context,
    tokens_response: Dict[str, str],
    tmp_path: Path,
    decoded_token: Dict[str, Any],
):
    decode_token.return_value = decoded_token
    context._token_cache = tmp_path
    client = httpx.Client(
        auth=TiledAuth(
            context.server_info.authentication.links.refresh_session,
            context.http_client.cookies["tiled_csrf"],
            context._token_directory(),
            context.client_id,
        ),
        cookies=context.http_client.cookies,
    )
    assert isinstance(client.auth, TiledAuth)
    client.auth.sync_tokens(tokens_response)
    client = from_context(context)
    return client


@patch("tiled.authenticators.OIDCAuthenticator.decode_token")
def test_whoami_endpoint(
    decode_token: MagicMock,
    client,
    decoded_token: Dict[str, Any],
):
    decode_token.return_value = decoded_token
    info = client.context.whoami()
    assert info == Principal(
        uuid=UUID(decoded_token["sub"]),
        type=PrincipalType.external,
        api_keys=[],
        identities=[],
    ).model_dump(mode="json")


@patch("tiled.authenticators.OIDCAuthenticator.decode_token")
def test_client_refresh(
    decode_token: MagicMock,
    context,
    tokens_response: Dict[str, str],
    tmp_path: Path,
    decoded_token: Dict[str, Any],
):
    decode_token.return_value = decoded_token
    context._token_cache = tmp_path
    httpx_client = httpx.Client(
        auth=TiledAuth(
            context.server_info.authentication.links.refresh_session,
            context.http_client.cookies["tiled_csrf"],
            context._token_directory(),
            context.client_id,
        ),
        cookies=context.http_client.cookies,
    )
    assert isinstance(httpx_client.auth, TiledAuth)
    httpx_client.auth.sync_tokens(tokens_response)
    client = from_context(context)
    base_url = str(context.http_client.base_url).replace("b'", "").replace("'", "")

    with respx.mock:
        # Change to httpx Client from TestClient FastAPI to mock responses
        context.http_client = httpx_client

        respx.post(
            context.server_info.authentication.links.refresh_session,
            data={
                "client_id": context.client_id,
                "grant_type": "refresh_token",
                "refresh_token": tokens_response["refresh_token"],
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        ).mock(
            return_value=httpx.Response(
                status_code=httpx.codes.OK, json=tokens_response
            )
        )
        respx.get(f"{base_url}/api/v1/auth/whoami").mock(
            side_effect=[
                httpx.Response(status_code=httpx.codes.UNAUTHORIZED),
                httpx.Response(status_code=httpx.codes.OK, json={}),
            ]
        )
        assert client.context.whoami() == {}


@patch("tiled.authenticators.OIDCAuthenticator.decode_token")
def test_logout(
    decode_token: MagicMock,
    context,
    tokens_response: Dict[str, str],
    tmp_path: Path,
    decoded_token: Dict[str, Any],
):
    decode_token.return_value = decoded_token
    context._token_cache = tmp_path
    httpx_client = httpx.Client(
        auth=TiledAuth(
            context.server_info.authentication.links.refresh_session,
            context.http_client.cookies["tiled_csrf"],
            context._token_directory(),
        ),
        cookies=context.http_client.cookies,
    )
    assert isinstance(httpx_client.auth, TiledAuth)
    httpx_client.auth.sync_tokens(tokens_response)
    client = from_context(context)

    with respx.mock:
        # Change to httpx Client from TestClient FastAPI to mock responses
        context.http_client = httpx_client

        respx.get(
            context.server_info.authentication.links.logout,
            params={
                "id_token_hint": tokens_response["id_token"],
                "client_id": context.client_id,
            },
        ).mock(return_value=httpx.Response(status_code=httpx.codes.OK))

        client.logout()
