import textwrap
from pathlib import Path
from typing import Any, Dict, Generator, List, Union
from unittest.mock import MagicMock, patch

import httpx
import pytest
import respx
import stamina
from respx import MockRouter

from tiled.adapters.mapping import MapAdapter
from tiled.client import Context
from tiled.client.auth import TiledAuth
from tiled.client.constructors import from_context
from tiled.client.context import device_code_grant, prompt_for_credentials
from tiled.server.app import build_app_from_config

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
        "authorize_endpoint": f"{context.http_client.base_url}/api/v1/auth/provider/keycloak_oidc/authorize",
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

        Waiting....

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
    assert info["identities"][0]["id"] == decoded_token["sub"]


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


@pytest.fixture
def tiled_mediated_urls(base_url: str) -> Dict[str, str]:
    return {
        # Tiled server endpoint the client POSTs to in order to *start* the flow.
        "auth_endpoint": f"{base_url}api/v1/auth/provider/keycloak_oidc/authorize",
        # Third-party provider login page the *user* visits in a browser.
        "authorization_uri": (
            f"{base_url}protocol/openid-connect/auth"
            "?client_id=tiled&response_type=code&scope=openid"
            f"&redirect_uri={base_url}api/v1/auth/provider/keycloak_oidc/device_code"
        ),
        # Tiled's own /token endpoint the *client* polls for tokens.
        "verification_uri": f"{base_url}api/v1/auth/provider/keycloak_oidc/token",
    }


@pytest.fixture
def mock_tiled_mediated_server(
    respx_mock: MockRouter,
    tiled_mediated_urls: Dict[str, str],
    tokens_response: Dict[str, Union[str, int]],
) -> MockRouter:
    """Mock the two Tiled server endpoints used by the "else" branch of
    device_code_grant used by ExternalAuthenticator providers such as
    OIDCAuthenticator, SAMLAuthenticator, ORCID, and Globus.

    In this branch, the *server* returns a payload with two URLs:
       - "authorization_uri": the third-party provider login page that the USER
         opens in a browser
       - "verification_uri": Tiled's own /token endpoint that the CLIENT polls
         (POSTing the device_code) to obtain tokens
    """
    device_code = "FsYWEv-Fl4wkFlrtp-EWH7HR1pkCG2NIfBNeUKlZBAY"
    user_code = "LCWE-ROXW"

    # 1. POST to Tiled's /authorize starts the flow and returns the two URIs.
    respx_mock.post(tiled_mediated_urls["auth_endpoint"], name="authorize").mock(
        return_value=httpx.Response(
            status_code=httpx.codes.OK,
            json={
                "authorization_uri": tiled_mediated_urls["authorization_uri"],
                "verification_uri": tiled_mediated_urls["verification_uri"],
                "interval": 5,
                "device_code": device_code,
                "expires_in": 600,
                "user_code": user_code,
            },
        )
    )

    # 2. POST to Tiled's /token endpoint (the client polls this) returns tokens.
    respx_mock.post(tiled_mediated_urls["verification_uri"], name="token_polling").mock(
        return_value=httpx.Response(
            status_code=httpx.codes.OK,
            json=tokens_response,
        )
    )
    return respx_mock


@patch("tiled.client.context.time.sleep")
def test_tiled_mediated_device_flow_browser_and_polling_urls(
    _sleep: MagicMock,
    mock_tiled_mediated_server: MockRouter,
    tiled_mediated_urls: Dict[str, str],
    tokens_response: Dict[str, Union[str, int]],
    capsys,
):
    """In the Tiled-mediated (non-oauth2_spec) device flow, the
    browser must be opened with the provider's ``authorization_uri`` and the
    client must poll Tiled's ``verification_uri`` (the /token endpoint).
    """
    with patch("webbrowser.open", return_value=False) as mock_open:
        tokens = device_code_grant(
            httpx.Client(),
            auth_endpoint=tiled_mediated_urls["auth_endpoint"],
            client_id=None,  # forces the Tiled-mediated "else" branch
            token_endpoint=None,  # forces the Tiled-mediated "else" branch
        )

    assert tokens == tokens_response

    # The browser is opened with the third-party provider's authorization_uri,
    # NOT with Tiled's /token endpoint.
    mock_open.assert_called_once_with(tiled_mediated_urls["authorization_uri"])

    # The URL printed for the user to visit is the authorization_uri.
    out, _err = capsys.readouterr()
    assert tiled_mediated_urls["authorization_uri"] in out
    assert tiled_mediated_urls["verification_uri"] not in out

    # The client polled Tiled's /token endpoint (verification_uri), and did NOT
    # POST to the provider's authorization page.
    token_polling_route = mock_tiled_mediated_server["token_polling"]
    assert token_polling_route.called
    assert token_polling_route.call_count == 1

    # Sanity check: the polling request carried the device_code as JSON.
    polled_request = token_polling_route.calls.last.request
    assert b"device_code" in polled_request.content


@patch("tiled.client.context.time.sleep")
def test_tiled_mediated_device_flow_polling_pending(
    _sleep: MagicMock,
    mock_tiled_mediated_server: MockRouter,
    tiled_mediated_urls: Dict[str, str],
    tokens_response: Dict[str, Union[str, int]],
):
    """The client should keep polling Tiled's /token endpoint while it returns
    ``authorization_pending`` and succeed once tokens are returned.

    Tiled's /token endpoint nests the error under ``detail`` (unlike the
    oauth2-spec branch), so the client reads ``response.json()["detail"]["error"]``.
    """
    token_polling_route = mock_tiled_mediated_server["token_polling"]
    token_polling_route.return_value = None
    token_polling_route.side_effect = [
        httpx.Response(
            status_code=httpx.codes.BAD_REQUEST,
            json={"detail": {"error": "authorization_pending"}},
        ),
        httpx.Response(status_code=httpx.codes.OK, json=tokens_response),
    ]

    stamina.set_testing(testing=True, attempts=1)

    with patch("webbrowser.open", return_value=False):
        tokens = device_code_grant(
            httpx.Client(),
            auth_endpoint=tiled_mediated_urls["auth_endpoint"],
            client_id=None,
            token_endpoint=None,
        )

    assert tokens == tokens_response
    assert token_polling_route.call_count == 2
