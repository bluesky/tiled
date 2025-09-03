import contextlib
import time
from collections.abc import Generator
from typing import Any, Literal, Mapping

from fastapi import Request, WebSocket
from starlette.types import Scope

from ..access_control.access_policies import NO_ACCESS
from ..adapters.mapping import MapAdapter

EMPTY_NODE = MapAdapter({})
API_KEY_COOKIE_NAME = "tiled_api_key"
API_KEY_QUERY_PARAMETER = "api_key"
CSRF_COOKIE_NAME = "tiled_csrf"


@contextlib.contextmanager
def record_timing(metrics: dict[str, Any], key: str) -> Generator[None]:
    """
    Set timings[key] equal to the run time (in milliseconds) of the context body.
    """
    t0 = time.perf_counter()
    yield
    metrics[key]["dur"] += time.perf_counter() - t0  # Units: seconds


def get_root_url(request: Request) -> str:
    """
    URL at which the app is being server, including API and UI
    """
    return f"{get_root_url_low_level(request.headers, request.scope)}"


def get_root_url_websocket(websocket: WebSocket) -> str:
    return f"{get_root_url_low_level(websocket.headers, websocket.scope)}"


def get_base_url_websocket(websocket: WebSocket) -> str:
    return f"{get_root_url_websocket(websocket)}/api/v1"


def get_base_url(request: Request) -> str:
    """
    Base URL for the API
    """
    return f"{get_root_url(request)}/api/v1"


def get_zarr_url(request, version: Literal["v2", "v3"] = "v2"):
    """
    Base URL for the Zarr API
    """
    return f"{get_root_url(request)}/zarr/{version}"


def get_root_url_low_level(request_headers: Mapping[str, str], scope: Scope) -> str:
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


async def filter_for_access(
    entry, access_policy, principal, authn_access_tags, authn_scopes, scopes, metrics
):
    if access_policy is not None and hasattr(entry, "search"):
        with record_timing(metrics, "acl"):
            if hasattr(entry, "lookup_adapter") and entry.node.parent is None:
                # This conditional only catches for the MapAdapter->CatalogAdapter
                # transition, to cover MapAdapter's lack of access control.
                # It can be removed once MapAdapter goes away.
                if not set(scopes).issubset(
                    await access_policy.allowed_scopes(
                        entry, principal, authn_access_tags, authn_scopes
                    )
                ):
                    return (entry := EMPTY_NODE)

            queries = await access_policy.filters(
                entry, principal, authn_access_tags, authn_scopes, set(scopes)
            )
            if queries is NO_ACCESS:
                entry = EMPTY_NODE
            else:
                for query in queries:
                    entry = entry.search(query)
    return entry
