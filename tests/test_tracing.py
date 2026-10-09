"""In-process tests for the OpenTelemetry request tracing configured by
`tiled.server.app._setup_opentelemetry_tracing`.

These use an in-memory span exporter, so they need no running OpenTelemetry
Collector, Jaeger, or Tempo. OpenTelemetry's global tracer provider can only be
set once per process, so a single provider is installed for the whole module and
the exporter is cleared between tests.
"""
import contextvars
import logging
import os

import httpx
import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind

from tiled.client import Context
from tiled.client.utils import handle_error
from tiled.server.app import build_app_from_config
from tiled.server.logging_config import LOGGING_CONFIG, TraceIdFilter
from tiled.server.utils import request_trace_id

from .utils import error_router

pytest.importorskip("opentelemetry.sdk")

# The FastAPI instrumentation reads OTEL_PYTHON_FASTAPI_EXCLUDED_URLS once, at
# import time, into a module-level default that `instrument_app` uses. Set it
# before that module is first imported (which the tracing hook does lazily on
# the first traced `build_app`). In deployments this variable is likewise set
# in the environment before the process starts.
os.environ["OTEL_PYTHON_FASTAPI_EXCLUDED_URLS"] = "healthz,api/v1/metrics"

CONFIG = {
    "authentication": {"single_user_api_key": "secret"},
    "trees": [{"path": "/", "tree": "tiled.examples.generated_minimal:tree"}],
}
# A syntactically valid endpoint that is never actually contacted: the tracing
# hook reuses the in-memory provider installed below instead of creating an OTLP
# exporter, so no network traffic occurs.
ENDPOINT = "http://otel-collector.invalid:4318"

# Global library instrumentation installed by the tracing hook, which must be
# undone so it does not leak into other test modules.
_GLOBAL_INSTRUMENTORS = [
    ("opentelemetry.instrumentation.asyncpg", "AsyncPGInstrumentor"),
    ("opentelemetry.instrumentation.redis", "RedisInstrumentor"),
    ("opentelemetry.instrumentation.httpx", "HTTPXClientInstrumentor"),
]


@pytest.fixture(scope="module")
def span_exporter():
    exporter = InMemorySpanExporter()
    current = trace.get_tracer_provider()
    if isinstance(current, TracerProvider):
        # Another module already installed a real provider; attach to it.
        provider = current
    else:
        provider = TracerProvider()
        trace.set_tracer_provider(provider)
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    yield exporter
    for module, cls in _GLOBAL_INSTRUMENTORS:
        try:
            mod = __import__(module, fromlist=[cls])
            getattr(mod, cls)().uninstrument()
        except Exception:
            pass


@pytest.fixture(autouse=True)
def _clear_spans(span_exporter):
    span_exporter.clear()
    yield
    span_exporter.clear()


def _build_app(monkeypatch, *, endpoint=ENDPOINT, config=CONFIG):
    if endpoint is None:
        monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    else:
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", endpoint)
    return build_app_from_config(config)


def _is_descendant_of(span, ancestor_span_id, by_id):
    seen = set()
    cur = span
    while cur is not None and cur.parent is not None:
        parent_id = cur.parent.span_id
        if parent_id == ancestor_span_id:
            return True
        if parent_id in seen:
            break
        seen.add(parent_id)
        cur = by_id.get(parent_id)
    return False


def test_traced_request_emits_server_and_phase_spans(monkeypatch, span_exporter):
    app = _build_app(monkeypatch)
    with Context.from_app(app) as context:
        # Discard spans emitted while the Context was being set up, so we measure
        # exactly one request.
        span_exporter.clear()
        response = context.http_client.get("/api/v1/metadata/")
        assert response.status_code == 200
        spans = span_exporter.get_finished_spans()

    assert spans, "expected spans to be exported for a traced request"
    ids = [s.context.span_id for s in spans]
    assert len(ids) == len(set(ids)), "spans must not be duplicated"
    assert len({s.context.trace_id for s in spans}) == 1, "one request => one trace"

    roots = [s for s in spans if s.parent is None]
    assert len(roots) == 1, "expected exactly one root span"
    assert roots[0].kind == SpanKind.SERVER, "root should be the FastAPI server span"

    by_id = {s.context.span_id: s for s in spans}
    names = {s.name for s in spans}
    assert "tiled.app" in names, "expected the per-request phase span 'tiled.app'"
    app_span = next(s for s in spans if s.name == "tiled.app")
    assert _is_descendant_of(
        app_span, roots[0].context.span_id, by_id
    ), "phase spans should be children of the request's server span"


def test_excluded_endpoint_emits_no_spans(monkeypatch, span_exporter):
    app = _build_app(monkeypatch)
    with Context.from_app(app) as context:
        span_exporter.clear()
        response = context.http_client.get("/healthz")
        assert response.status_code == 200
        assert (
            not span_exporter.get_finished_spans()
        ), "excluded endpoints must not produce spans (no orphan traces)"


def test_tracing_disabled_by_default(monkeypatch):
    # Without OTEL_EXPORTER_OTLP_ENDPOINT the tracing hook returns early and does
    # not instrument the app, so tracing is off and adds no overhead. (Asserting
    # on emitted spans is not reliable here: FastAPI >=0.142 ships its own
    # telemetry that emits request spans on any globally installed provider when
    # the app is not instrumented by OpenTelemetry.)
    app = _build_app(monkeypatch, endpoint=None)
    assert not getattr(app, "_is_instrumented_by_opentelemetry", False)
    with Context.from_app(app) as context:
        response = context.http_client.get("/api/v1/metadata/")
    assert "x-tiled-trace-id" not in response.headers


def test_no_duplicate_export_pipeline(monkeypatch, span_exporter):
    """Guard against FastAPI's built-in telemetry (>=0.142) registering a second
    OTLP export pipeline, which would export every span twice."""
    from starlette.testclient import TestClient

    provider = trace.get_tracer_provider()
    processors = provider._active_span_processor._span_processors
    before = len(processors)

    app = _build_app(monkeypatch)
    # Entering the TestClient runs the ASGI lifespan; FastAPI configures its
    # built-in telemetry on `lifespan.startup`.
    with TestClient(app):
        pass

    after = len(provider._active_span_processor._span_processors)
    assert after == before, (
        "a second span processor was registered on the global provider; "
        "FastAPI's built-in OpenTelemetry auto-configuration is not disabled"
    )


# --- trace ID in responses and logs -------------------------------------------


@pytest.mark.parametrize(
    "path, status_code", [("/api/v1/metadata/", 200), ("/error", 500)]
)
def test_response_carries_trace_id(monkeypatch, span_exporter, path, status_code):
    """The trace ID is in the response header, exposed to cross-origin browser
    clients. Unhandled exceptions (500) are answered by the exception handler,
    outside the middleware that adds the header, so the handler adds it, and the
    client includes it in the error message. (Starlette answers those outside the
    CORS middleware too, so they carry no CORS headers at all.)"""
    origin = "https://example.com"
    app = _build_app(monkeypatch, config={**CONFIG, "allow_origins": [origin]})
    app.include_router(error_router)
    with Context.from_app(app, raise_server_exceptions=False) as context:
        span_exporter.clear()
        response = context.http_client.get(path, headers={"Origin": origin})
        spans = span_exporter.get_finished_spans()

    assert response.status_code == status_code
    server_span = next(s for s in spans if s.kind == SpanKind.SERVER)
    trace_id = trace.format_trace_id(server_span.context.trace_id)
    assert response.headers["x-tiled-trace-id"] == trace_id
    # The correlation ID is on the server span, so it also finds the trace.
    assert (
        server_span.attributes["tiled.request_id"]
        == response.headers["x-tiled-request-id"]
    )
    if status_code == 200:
        exposed = response.headers["access-control-expose-headers"].lower()
        assert "x-tiled-trace-id" in exposed
    else:
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            handle_error(response)
        assert f"trace ID {trace_id}" in exc_info.value.args[0]


@pytest.mark.parametrize("in_span", [True, False])
@pytest.mark.parametrize("traced", [True, False])
def test_log_lines_carry_trace_id(span_exporter, traced, in_span):
    """Log lines carry the trace ID next to the correlation ID, also after the
    request's span ended (e.g. uvicorn's traceback of an unhandled exception), and
    are unchanged when the request is not traced."""
    formatter = logging.Formatter(LOGGING_CONFIG["formatters"]["default"]["format"])

    def log_line():
        record = logging.LogRecord("tiled", logging.INFO, "", 0, "message", None, None)
        record.correlation_id = "0123456789abcdef"
        record.levelprefix = "INFO:"
        TraceIdFilter().filter(record)
        return formatter.format(record)

    def handle_request():
        with trace.get_tracer(__name__).start_as_current_span("request") as span:
            # What TraceIdMiddleware does at the start of a traced request.
            request_trace_id.set(
                trace.format_trace_id(span.get_span_context().trace_id)
            )
            line = log_line()
        return line if in_span else log_line()

    # A copy of the context, so request_trace_id does not leak into other tests.
    if traced:
        line = contextvars.copy_context().run(handle_request)
        assert line.startswith("[0123456789abcdef ") and len(line.split()[1]) == 33
    else:
        line = contextvars.copy_context().run(log_line)
        assert line == "[0123456789abcdef] INFO: message"
