"""In-process tests for the OpenTelemetry request tracing configured by
``tiled.server.app._setup_opentelemetry_tracing``.

These use an in-memory span exporter, so they need no running OpenTelemetry
Collector, Jaeger, or Tempo. OpenTelemetry's global tracer provider can only be
set once per process, so a single provider is installed for the whole module and
the exporter is cleared between tests.
"""
import os

import pytest

pytest.importorskip("opentelemetry.sdk")

# The FastAPI instrumentation reads OTEL_PYTHON_FASTAPI_EXCLUDED_URLS once, at
# import time, into a module-level default that ``instrument_app`` uses. Set it
# before that module is first imported (which the tracing hook does lazily on
# the first traced ``build_app``). In deployments this variable is likewise set
# in the environment before the process starts.
os.environ["OTEL_PYTHON_FASTAPI_EXCLUDED_URLS"] = "healthz,api/v1/metrics"

from opentelemetry import trace  # noqa: E402
from opentelemetry.sdk.trace import TracerProvider  # noqa: E402
from opentelemetry.sdk.trace.export import SimpleSpanProcessor  # noqa: E402
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (  # noqa: E402
    InMemorySpanExporter,
)
from opentelemetry.trace import SpanKind  # noqa: E402

from tiled.client import Context, from_context  # noqa: E402
from tiled.server.app import build_app_from_config  # noqa: E402

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


def _build_app(monkeypatch, *, endpoint=ENDPOINT):
    if endpoint is None:
        monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    else:
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", endpoint)
    return build_app_from_config(CONFIG)


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
    assert _is_descendant_of(app_span, roots[0].context.span_id, by_id), (
        "phase spans should be children of the request's server span"
    )


def test_excluded_endpoint_emits_no_spans(monkeypatch, span_exporter):
    app = _build_app(monkeypatch)
    with Context.from_app(app) as context:
        span_exporter.clear()
        response = context.http_client.get("/healthz")
        assert response.status_code == 200
        assert not span_exporter.get_finished_spans(), (
            "excluded endpoints must not produce spans (no orphan traces)"
        )


def test_tracing_disabled_by_default_emits_no_spans(monkeypatch, span_exporter):
    app = _build_app(monkeypatch, endpoint=None)
    with Context.from_app(app) as context:
        span_exporter.clear()
        response = context.http_client.get("/api/v1/metadata/")
        assert response.status_code == 200
        assert not span_exporter.get_finished_spans(), (
            "without OTEL_EXPORTER_OTLP_ENDPOINT the app should not be traced"
        )


def test_no_duplicate_export_pipeline(monkeypatch, span_exporter):
    """Guard against FastAPI's built-in telemetry (>=0.142) registering a second
    OTLP export pipeline, which would export every span twice."""
    from starlette.testclient import TestClient

    provider = trace.get_tracer_provider()
    processors = provider._active_span_processor._span_processors
    before = len(processors)

    app = _build_app(monkeypatch)
    # Entering the TestClient runs the ASGI lifespan; FastAPI configures its
    # built-in telemetry on ``lifespan.startup``.
    with TestClient(app):
        pass

    after = len(provider._active_span_processor._span_processors)
    assert after == before, (
        "a second span processor was registered on the global provider; "
        "FastAPI's built-in OpenTelemetry auto-configuration is not disabled"
    )
