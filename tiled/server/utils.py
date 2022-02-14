import contextlib
import time

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


def get_base_url(request):
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
    host = request.headers.get("x-forwarded-host", request.headers["host"])
    scheme = request.headers.get("x-forwarded-proto", request.url.scheme)
    root_path = request.scope.get("root_path") or "/"
    if not root_path.endswith("/"):
        root_path = f"{root_path}/"
    return f"{scheme}://{host}{root_path}"
