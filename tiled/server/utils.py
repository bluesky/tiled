import contextlib
import time
from datetime import datetime, timezone
from typing import Optional

from fastapi import Depends, HTTPException, Request, Security
from fastapi.openapi.models import APIKey, APIKeyIn
from fastapi.security import SecurityScopes
from fastapi.security.api_key import APIKeyBase, APIKeyCookie, APIKeyQuery
from fastapi.security.utils import get_authorization_scheme_param
from starlette.status import HTTP_400_BAD_REQUEST

from ..access_policies import NO_ACCESS
from ..adapters.mapping import MapAdapter

EMPTY_NODE = MapAdapter({})
API_KEY_COOKIE_NAME = "tiled_api_key"
API_KEY_QUERY_PARAMETER = "api_key"
CSRF_COOKIE_NAME = "tiled_csrf"


def utcnow():
    "UTC now with second resolution"
    return datetime.now(timezone.utc).replace(microsecond=0)


def headers_for_401(request: Request, security_scopes: SecurityScopes):
    # call directly from methods, rather than as a dependency, to avoid calling
    # when not needed.
    if security_scopes.scopes:
        authenticate_value = f'Bearer scope="{security_scopes.scope_str}"'
    else:
        authenticate_value = "Bearer"
    return {
        "WWW-Authenticate": authenticate_value,
        "X-Tiled-Root": get_base_url(request),
    }


class APIKeyAuthorizationHeader(APIKeyBase):
    """
    Expect a header like

    Authorization: Apikey SECRET

    where Apikey is case-insensitive.
    """

    def __init__(
        self,
        *,
        name: str,
        scheme_name: Optional[str] = None,
        description: Optional[str] = None,
    ):
        self.model: APIKey = APIKey(
            **{"in": APIKeyIn.header}, name=name, description=description
        )
        self.scheme_name = scheme_name or self.__class__.__name__

    async def __call__(self, request: Request) -> Optional[str]:
        authorization: str = request.headers.get("Authorization")
        scheme, param = get_authorization_scheme_param(authorization)
        if not authorization or scheme.lower() == "bearer":
            return None
        if scheme.lower() != "apikey":
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail=(
                    "Authorization header must include the authorization type "
                    "followed by a space and then the secret, as in "
                    "'Bearer SECRET' or 'Apikey SECRET'. "
                ),
            )
        return param


async def get_api_key(
    api_key_query: str = Security(APIKeyQuery(name="api_key", auto_error=False)),
    api_key_header: str = Security(
        APIKeyAuthorizationHeader(
            name="Authorization",
            description="Prefix value with 'Apikey ' as in, 'Apikey SECRET'",
        )
    ),
    api_key_cookie: str = Security(
        APIKeyCookie(name=API_KEY_COOKIE_NAME, auto_error=False)
    ),
) -> Optional[str]:
    for api_key in [api_key_query, api_key_header, api_key_cookie]:
        if api_key is not None:
            return api_key
    return None


@contextlib.contextmanager
def record_timing(metrics, key):
    """
    Set timings[key] equal to the run time (in milliseconds) of the context body.
    """
    t0 = time.perf_counter()
    yield
    metrics[key]["dur"] += time.perf_counter() - t0  # Units: seconds


def get_root_url(request):
    """
    URL at which the app is being server, including API and UI
    """
    return f"{get_root_url_low_level(request.headers, request.scope)}"


def get_base_url(request):
    """
    Base URL for the API
    """
    return f"{get_root_url(request)}/api/v1"


def get_root_url_low_level(request_headers, scope):
    # We want to get the scheme, host, and root_path (if any)
    # *as it appears to the client* for use in assembling links to
    # include in our responses.
    #
    # We need to consider:
    #
    # * FastAPI may be behind a load balancer, such that for a client request
    #   like "https://example.com/..." the Host header is set to something
    #   like "localhost:8000" and the request.url.scheme is "http".
    #   We consult X-Forwarded-* headers to get the original Host and scheme.
    #   Note that, although these are a de facto standard, they may not be
    #   set by default. With nginx, for example, they need to be configured.
    #
    # * The client may be connecting through SSH port-forwarding. (This
    #   is a niche use case but one that we nonetheless care about.)
    #   The Host or X-Forwarded-Host header may include a non-default port.
    #   The HTTP spec specifies that the Host header may include a port
    #   to specify a non-default port.
    #   https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.23
    host = request_headers.get("x-forwarded-host", request_headers["host"])
    scheme = request_headers.get("x-forwarded-proto", scope["scheme"])
    root_path = scope.get("root_path", "")
    if root_path.endswith("/"):
        root_path = root_path[:-1]
    return f"{scheme}://{host}{root_path}"


async def filter_for_access(entry, principal, scopes, metrics, path_parts):
    access_policy = getattr(entry, "access_policy", None)
    if access_policy is not None:
        with record_timing(metrics, "acl"):
            queries = await entry.access_policy.filters(
                entry, principal, set(scopes), path_parts
            )
            if queries is NO_ACCESS:
                entry = EMPTY_NODE
            else:
                for query in queries:
                    entry = entry.search(query)
    return entry


async def move_api_key(
    request: Request,
    api_key: Optional[str] = Depends(get_api_key),
):
    """
    Moves API key if given as a query parameter into a cookie
    """

    if (
        api_key is not None
        and "api_key" in request.query_params
        and request.cookies.get(API_KEY_COOKIE_NAME) != api_key
    ):
        request.state.cookies_to_set.append(
            {"key": API_KEY_COOKIE_NAME, "value": api_key}
        )
        return api_key
