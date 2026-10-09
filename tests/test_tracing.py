"""In-process tests for Tiled's OpenTelemetry tracing.

Two groups, both capturing spans with an in-memory exporter (no Collector,
Jaeger, or Tempo needed):

* request-level tracing configured by
  `tiled.server.app._setup_opentelemetry_tracing` -- the server span and phase
  spans, excluded endpoints, off-by-default, and no duplicate export pipeline;
* external-service instrumentation -- asyncpg (catalog Postgres), ADBC (SQL
  storage), Redis (streaming cache), and httpx (webhook delivery) -- exercised
  against live backends, which skip when the backend is not configured via
  `TILED_TEST_POSTGRESQL_URI` / `TILED_TEST_REDIS`.

OpenTelemetry's global tracer provider can only be set once per process, so a
single provider is installed for the whole module and the exporter is cleared
between tests.
"""
import os
from urllib.parse import urlparse

import numpy as np
import pyarrow
import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind

from tiled.catalog import in_memory
from tiled.client import Context, from_context
from tiled.config import Authentication, WebhooksConfig
from tiled.server.app import build_app, build_app_from_config
from tiled.server.schemas import WebhookRegistrationRequest

# The FastAPI instrumentation reads OTEL_PYTHON_FASTAPI_EXCLUDED_URLS once, at
# import time, into a module-level default that `instrument_app` uses. Set it
# before that module is first imported (which the tracing hook does lazily on
# the first traced `build_app`). In deployments this variable is likewise set
# in the environment before the process starts.
os.environ["OTEL_PYTHON_FASTAPI_EXCLUDED_URLS"] = "healthz,api/v1/metrics"

# Minimal in-memory tree used by the request-level tests.
CONFIG = {
    "authentication": {"single_user_api_key": "secret"},
    "trees": [{"path": "/", "tree": "tiled.examples.generated_minimal:tree"}],
}
# A syntactically valid endpoint that is never actually contacted: the tracing
# hook reuses the in-memory provider installed below instead of creating an OTLP
# exporter, so no network traffic occurs. We only declare it to turn tracing on.
ENDPOINT = "http://otel-collector.invalid:4318"
API_KEY = "secret"
# respx mocks this, so no real delivery happens (and HTTPS keeps the default
# URL validator happy without allowing http/private targets).
WEBHOOK_URL = "https://webhook.example.com/tiled-events"

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


def _enable_tracing(monkeypatch):
    # Set before build_app so the tracing hook runs and instruments the
    # libraries against the in-memory provider.
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", ENDPOINT)


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


def _spans_where(spans, key, value):
    return [s for s in spans if s.attributes.get(key) == value]


# --- request-level tracing -------------------------------------------------


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


# --- external-service spans (live backends; skip when not configured) ------


def test_catalog_query_emits_asyncpg_spans(
    monkeypatch, span_exporter, postgres_uri, tmp_path
):
    """Catalog access over asyncpg produces postgresql client spans."""
    _enable_tracing(monkeypatch)
    config = {
        "authentication": {"single_user_api_key": API_KEY},
        "trees": [
            {
                "tree": "catalog",
                "path": "/",
                "args": {
                    "uri": postgres_uri,
                    "writable_storage": [str(tmp_path / "data")],
                    "init_if_not_exists": True,
                },
            }
        ],
    }
    with Context.from_app(build_app_from_config(config)) as context:
        client = from_context(context)
        client.write_array(np.arange(5), key="arr")
        span_exporter.clear()
        list(client)  # a search -> catalog SELECT over asyncpg

    spans = span_exporter.get_finished_spans()
    pg_spans = _spans_where(spans, "db.system", "postgresql")
    assert pg_spans, "expected asyncpg (postgresql) spans for the catalog query"
    # The catalog database name appears on the span so it is distinguishable in the service graph
    assert any(s.attributes.get("db.name") for s in pg_spans)


def test_sql_storage_write_emits_adbc_spans(
    monkeypatch, span_exporter, sql_storage_uri, tmp_path
):
    """Writing an appendable table to SQL storage (SQLite, DuckDB, or Postgres)
    produces both the manual `adbc_ingest` span (the bulk write bypasses the DBAPI
    `execute` path) and the DBAPI-level spans from the instrumented ADBC
    connection."""
    _enable_tracing(monkeypatch)
    dialect = urlparse(sql_storage_uri).scheme
    config = {
        "authentication": {"single_user_api_key": API_KEY},
        "trees": [
            {
                "tree": "catalog",
                "path": "/",
                "args": {
                    "uri": f"sqlite:///{tmp_path / 'catalog.db'}",
                    "writable_storage": [sql_storage_uri],
                    "init_if_not_exists": True,
                },
            }
        ],
    }
    table = pyarrow.Table.from_pydict({"A": [1, 2, 3], "B": [4, 5, 6]})
    with Context.from_app(build_app_from_config(config)) as context:
        client = from_context(context)
        span_exporter.clear()
        appendable = client.create_appendable_table(schema=table.schema, key="tab")
        appendable.append_partition(0, table)
        # Tracing must not break the storage connections.
        assert appendable.read()["A"].tolist() == [1, 2, 3]

    spans = span_exporter.get_finished_spans()
    ingest = [s for s in spans if s.name == "adbc_ingest"]
    assert ingest, "expected the manual adbc_ingest span"
    assert ingest[0].kind == SpanKind.CLIENT
    assert ingest[0].attributes.get("db.system") == dialect

    # The ADBC connection factory is wrapped by _instrument_adbc_creator, so the
    # DBAPI-level statements (e.g. the CREATE TABLE preceding the ingest) are
    # also traced.
    dbapi_spans = [
        s for s in _spans_where(spans, "db.system", dialect) if s.name != "adbc_ingest"
    ]
    assert dbapi_spans, "expected DBAPI-instrumented storage query spans"
    # They carry the database name (`adbc_current_catalog`), except on DuckDB, whose
    # ADBC driver does not implement it.
    if dialect != "duckdb":
        assert any(s.attributes.get("db.name") for s in dbapi_spans)


def test_streaming_emits_redis_spans(monkeypatch, span_exporter, redis_uri, tmp_path):
    """Subscribing to a node's stream exercises the Redis streaming cache."""
    _enable_tracing(monkeypatch)
    config = {
        "authentication": {"single_user_api_key": API_KEY},
        "trees": [
            {
                "tree": "catalog",
                "path": "/",
                "args": {
                    "uri": "sqlite:///:memory:",
                    "writable_storage": [str(tmp_path / "data")],
                    "init_if_not_exists": True,
                },
            }
        ],
        "streaming_cache": {
            "uri": redis_uri,
            "data_ttl": 600,
            "seq_ttl": 600,
            "socket_timeout": 600,
            "socket_connect_timeout": 10,
        },
    }
    with Context.from_app(build_app_from_config(config)) as context:
        client = from_context(context)
        test_client = context.http_client  # the underlying starlette TestClient
        node = client.write_array(np.arange(10), key="stream_node")
        span_exporter.clear()
        with test_client.websocket_connect(
            "/api/v1/stream/single/stream_node?envelope_format=json",
            headers={"Authorization": f"Apikey {API_KEY}"},
        ):
            node.write(np.arange(10) + 1)

    spans = span_exporter.get_finished_spans()
    redis_spans = _spans_where(spans, "db.system", "redis")
    assert redis_spans, "expected Redis spans for the streaming subscription"


def test_webhook_delivery_emits_httpx_span(monkeypatch, span_exporter, tmp_path):
    """Delivering a webhook goes through httpx, producing an outbound client
    span. No external backend is needed; the delivery is mocked with respx."""
    respx = pytest.importorskip("respx")
    from unittest.mock import patch

    from httpx import Response

    _enable_tracing(monkeypatch)
    tree = in_memory(writable_storage=[f"file://localhost{tmp_path / 'data'}"])
    app = build_app(
        tree,
        authentication=Authentication(single_user_api_key=API_KEY),
        # A non-None webhooks config enables the delivery dispatcher.
        server_settings={"webhooks": WebhooksConfig(secret_keys=["test-webhook-key"])},
    )

    with Context.from_app(app) as context:
        client = from_context(context)
        # respx mocks the delivery; patching the SSRF check lets us register an example.com target
        with respx.mock, patch("tiled.server.webhook_router.check_url_ssrf_safety"):
            respx.post(WEBHOOK_URL).mock(return_value=Response(200))
            context.http_client.post(
                "/api/v1/webhooks/target/",
                json=WebhookRegistrationRequest(url=WEBHOOK_URL).model_dump(
                    mode="json"
                ),
            ).raise_for_status()
            span_exporter.clear()
            client.create_container("triggers_webhook")

    spans = span_exporter.get_finished_spans()
    httpx_spans = [
        s
        for s in spans
        if s.kind == SpanKind.CLIENT
        and (
            s.attributes.get("http.method") == "POST"
            or s.attributes.get("http.request.method") == "POST"
        )
    ]
    assert httpx_spans, "expected an outbound httpx client span for the webhook POST"
    # The span targets the webhook URL's host.
    urls = [
        str(s.attributes.get("http.url") or s.attributes.get("url.full") or "")
        for s in httpx_spans
    ]
    assert any("webhook.example.com" in u for u in urls)
