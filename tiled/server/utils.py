import contextlib
import importlib.util
import time
from collections.abc import Generator
from typing import Any, Literal, Mapping, Optional, Sequence

from fastapi import Request, WebSocket
from starlette.types import Scope

from ..access_control.access_policies import NO_ACCESS
from ..access_control.protocols import AccessPolicy
from ..adapters.mapping import MapAdapter
from ..queries import AccessTagsFilter
from ..server.schemas import Principal
from ..type_aliases import AccessTags, Scopes

EMPTY_NODE = MapAdapter({})
API_KEY_COOKIE_NAME = "tiled_api_key"
API_KEY_QUERY_PARAMETER = "api_key"
CSRF_COOKIE_NAME = "tiled_csrf"

# Human-readable OpenTelemetry span names for the phases timed below.
_SPAN_NAMES = {
    "app": "tiled.app",
    "acl": "tiled.access_control",
    "read": "tiled.read",
    "tok": "tiled.tokenize",
    "pack": "tiled.pack",
}

# Enable tracing if the OpenTelemetry API is installed. `opentelemetry` is a
# namespace package shared by all `opentelemetry-*` distributions, so check for
# the `trace` module itself and its parent (first).
_tracer = None
if importlib.util.find_spec("opentelemetry") and importlib.util.find_spec(
    "opentelemetry.trace"
):
    from opentelemetry import trace

    _tracer = trace.get_tracer("tiled.server")


def normalize_root_path(root_path: Optional[str]) -> str:
    """Coerce a root_path to "" or "/prefix" (no trailing slash)."""
    stripped = (root_path or "").strip("/")
    return f"/{stripped}" if stripped else ""


@contextlib.contextmanager
def record_timing(metrics: dict[str, Any], key: str) -> Generator[None]:
    """
    Set timings[key] equal to the run time (in seconds) of the context body.

    When there is an active recording trace span (i.e. this request is being
    traced), also open a child OpenTelemetry span around the body so these
    phases appear in the request's trace. Outside a traced request (tracing
    disabled, or an excluded endpoint such as health checks and metrics
    scrapes) no span is created, avoiding orphaned single-span traces.
    """
    if _tracer is not None and trace.get_current_span().is_recording():
        span = _tracer.start_as_current_span(_SPAN_NAMES.get(key, f"tiled.{key}"))
    else:
        span = contextlib.nullcontext()
    t0 = time.perf_counter()
    with span:
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


def get_current_url(request: Request) -> str:
    """
    Externally-visible URL of this request, without query params.
    """
    return f"{_get_origin(request.headers, request.scope)}{request.url.path}"


def get_zarr_url(request, version: Literal["v2", "v3"] = "v2"):
    """
    Base URL for the Zarr API
    """
    return f"{get_root_url(request)}/zarr/{version}"


def get_root_url_low_level(request_headers: Mapping[str, str], scope: Scope) -> str:
    # We want to get the scheme, host, and root_path (if any)
    # *as it appears to the client* for use in assembling links to
    # include in our responses.
    root_path = normalize_root_path(scope.get("root_path"))
    return f"{_get_origin(request_headers, scope)}{root_path}"


def _get_origin(request_headers: Mapping[str, str], scope: Scope) -> str:
    """
    Scheme and host as they appear to the client, without any root_path.
    """
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
    return f"{scheme}://{host}"


async def filter_for_access(
    entry,
    access_policy: Optional[AccessPolicy],
    principal: Principal,
    authn_access_tags: Optional[AccessTags],
    authn_scopes: Scopes,
    scopes: Sequence[str],
    metrics: dict[str, Any],
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
                    if isinstance(query, AccessTagsFilter) and hasattr(
                        entry, "resolve_access_tag_ids"
                    ):
                        query.tag_ids = await entry.resolve_access_tag_ids(query.tags)
                    entry = entry.search(query)
    return entry
