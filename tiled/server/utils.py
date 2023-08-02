import contextlib
import time

from ..access_policies import NO_ACCESS
from ..adapters.mapping import MapAdapter

EMPTY_NODE = MapAdapter({})
API_KEY_COOKIE_NAME = "tiled_api_key"
API_KEY_QUERY_PARAMETER = "api_key"
CSRF_COOKIE_NAME = "tiled_csrf"


def get_authenticators():
    raise NotImplementedError(
        "This should be overridden via dependency_overrides. "
        "See tiled.server.app.build_app()."
    )


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


def filter_for_access(entry, principal, scopes, metrics):
    access_policy = getattr(entry, "access_policy", None)
    if access_policy is not None:
        with record_timing(metrics, "acl"):
            queries = entry.access_policy.filters(entry, principal, set(scopes))
            if queries is NO_ACCESS:
                entry = EMPTY_NODE
            else:
                for query in queries:
                    entry = entry.search(query)
    return entry
