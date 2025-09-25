"""
Read https://prometheus.io/docs/practices/naming/ for naming
conventions for metrics & labels. We generally prefer naming them
`tiled_<noun>_<verb>_<type_suffix>`.
"""

from prometheus_client import Counter, Gauge, Histogram
from sqlalchemy import event
from sqlalchemy.pool import QueuePool

REQUEST_DURATION = Histogram(
    "tiled_request_duration_seconds",
    "request duration for all HTTP requests",
    ["method", "code", "endpoint"],
)
RESPONSE_SIZE = Histogram(
    "tiled_response_content_length_bytes",
    "response size in bytes for all HTTP requests",
    ["method", "code", "endpoint"],
    buckets=[
        1,
        10,
        100,
        1000,
        10_000,
        100_000,
        1_000_000,
        10_000_000,
        100_000_000,
        1_000_000_000,
        10_000_000_000,
        float("inf"),
    ],
)
ACL_DURATION = Histogram(
    "tiled_acl_duration_seconds",
    "time spent applying access control for all HTTP requests",
    ["method", "code", "endpoint"],
)
READ_DURATION = Histogram(
    "tiled_read_duration_seconds",
    "time spent reading data for all HTTP requests",
    ["method", "code", "endpoint"],
)
TOKENIZE_DURATION = Histogram(
    "tiled_tokenization_duration_seconds",
    "time spent generating content identifier for all HTTP requests",
    ["method", "code", "endpoint"],
)
PACK_DURATION = Histogram(
    "tiled_pack_duration_seconds",
    "time spent packing (serializing) data for all HTTP requests",
    ["method", "code", "endpoint"],
)
COMPRESSION_DURATION = Histogram(
    "tiled_compress_duration_seconds",
    "time spent applying compression for all HTTP requests",
    ["method", "code", "endpoint", "encoding"],
)
COMPRESSION_RATIO = Histogram(
    "tiled_compress_ratio",
    "compression ratio for all HTTP requests",
    ["method", "code", "endpoint", "encoding"],
    buckets=[1, 2.5, 5, 10, 15, 30, 60, 120, float("inf")],
)

# Database connections pool size metrics
DB_POOL_CONNECTED = Gauge(
    "tiled_db_pool_established",
    "Number of established connections",
    ["uri"],
)
DB_POOL_CHECKEDOUT = Gauge(
    "tiled_db_pool_active",
    "Number of currently active (in-use) connections checked out from the pool",
    ["uri"],
)
DB_POOL_OPENED_TOTAL = Counter(
    "tiled_db_pool_opened_total",
    "Total number of established connections over time",
    ["uri"],
)
DB_POOL_CLOSED_TOTAL = Counter(
    "tiled_db_pool_closed_total",
    "Total number of closed connections over time",
    ["uri"],
)
DB_POOL_CHECKOUTS_TOTAL = Counter(
    "tiled_db_pool_checkouts_total",
    "Total number of checked out connections from the pool over time",
    ["uri"],
)
DB_POOL_CHECKINS_TOTAL = Counter(
    "tiled_db_pool_checkins_total",
    "Total number of returned connections to the pool over time",
    ["uri"],
)
DB_POOL_INVALID_TOTAL = Counter(
    "tiled_db_pool_invalid_total",
    "Total number of invalidated connections over time",
    ["uri"],
)
DB_POOL_RESET_TOTAL = Counter(
    "tiled_db_pool_reset_total",
    "Total number of reset connections over time",
    ["uri"],
)
DB_POOL_FIRST_OVERFLOW_TOTAL = Counter(
    "tiled_db_pool_first_overflow_total",
    "Number of times a checkout caused the first overflow connection to be used",
    ["uri"],
)
DB_POOL_AT_MAX_TOTAL = Counter(
    "tiled_db_pool_at_max_total",
    "Number of times a checkout occurred when the pool was at its absolute capacity",  # noqa
    ["uri"],
)

# Initialize labels in advance so that the metrics exist (and can be used in
# dashboards and alerts) even if they have not yet occurred.
for code in ["200", "304", "500"]:
    for endpoint in {
        "about",
        "auth",
        "data",
        "entries",
        "metadata",
        "metrics",
        "search",
    }:
        REQUEST_DURATION.labels(code=code, method="GET", endpoint=endpoint)
        RESPONSE_SIZE.labels(code=code, method="GET", endpoint=endpoint)
        ACL_DURATION.labels(code=code, method="GET", endpoint=endpoint)
        READ_DURATION.labels(code=code, method="GET", endpoint=endpoint)
        TOKENIZE_DURATION.labels(code=code, method="GET", endpoint=endpoint)
        PACK_DURATION.labels(code=code, method="GET", endpoint=endpoint)
        for encoding in ["blosc2", "gzip", "lz4", "zstd"]:
            COMPRESSION_DURATION.labels(
                code=code, method="GET", endpoint=endpoint, encoding=encoding
            )
            COMPRESSION_RATIO.labels(
                code=code, method="GET", endpoint=endpoint, encoding=encoding
            )


def capture_request_metrics(request, response):
    method = request.method
    code = response.status_code
    metrics = request.state.metrics
    endpoint = getattr(request.state, "endpoint", "unknown")
    REQUEST_DURATION.labels(method=method, code=code, endpoint=endpoint).observe(
        metrics["app"]["dur"]
    )
    RESPONSE_SIZE.labels(method=method, code=code, endpoint=endpoint).observe(
        int(response.headers.get("content-length", 0))
    )
    if "acl" in metrics:
        ACL_DURATION.labels(method=method, code=code, endpoint=endpoint).observe(
            metrics["acl"]["dur"]
        )
    if "read" in metrics:
        READ_DURATION.labels(method=method, code=code, endpoint=endpoint).observe(
            metrics["read"]["dur"]
        )
    if "tok" in metrics:
        TOKENIZE_DURATION.labels(method=method, code=code, endpoint=endpoint).observe(
            metrics["tok"]["dur"]
        )
    if "pack" in metrics:
        PACK_DURATION.labels(method=method, code=code, endpoint=endpoint).observe(
            metrics["read"]["pack"]
        )
    if "compress" in metrics:
        encoding = response.headers.get("content-encoding")
        if encoding:
            COMPRESSION_DURATION.labels(
                method=method, code=code, endpoint=endpoint, encoding=encoding
            ).observe(metrics["compress"]["dur"])
            COMPRESSION_RATIO.labels(
                method=method, code=code, endpoint=endpoint, encoding=encoding
            ).observe(metrics["compress"]["ratio"])


def monitor_db_pool(pool: QueuePool, name: str):
    """Set up monitoring of a SQLAlchemy Engine's connection pool.

    Parameters
    ----------
        pool : sqlalchemy.pool.QueuePool
            The connection pool to monitor.
        name : str
            A name for this pool/engine, typically the sanitized database URI.
    """

    # Initialize the gauges and counters in advance so that they exist
    # (and can be used in dashboards and alerts)
    DB_POOL_CONNECTED.labels(name).set(0)
    DB_POOL_CHECKEDOUT.labels(name).set(0)
    DB_POOL_OPENED_TOTAL.labels(name).inc(0)
    DB_POOL_CLOSED_TOTAL.labels(name).inc(0)
    DB_POOL_CHECKOUTS_TOTAL.labels(name).inc(0)
    DB_POOL_CHECKINS_TOTAL.labels(name).inc(0)
    DB_POOL_INVALID_TOTAL.labels(name).inc(0)
    DB_POOL_RESET_TOTAL.labels(name).inc(0)
    DB_POOL_FIRST_OVERFLOW_TOTAL.labels(name).inc(0)
    DB_POOL_AT_MAX_TOTAL.labels(name).inc(0)

    @event.listens_for(pool, "connect")
    def on_connect(dbapi_connection, connection_record):
        DB_POOL_CONNECTED.labels(name).inc()
        DB_POOL_OPENED_TOTAL.labels(name).inc()

    @event.listens_for(pool, "close")
    def on_disconnect(dbapi_connection, connection_record):
        DB_POOL_CONNECTED.labels(name).dec()
        DB_POOL_CLOSED_TOTAL.labels(name).inc()

    @event.listens_for(pool, "checkout")
    def on_checkout(dbapi_connection, connection_record, connection_proxy):
        DB_POOL_CHECKEDOUT.labels(name).inc()
        DB_POOL_CHECKOUTS_TOTAL.labels(name).inc()

        # First overflow: we just used the very first overflow slot
        if pool.overflow() == 1:
            DB_POOL_FIRST_OVERFLOW_TOTAL.labels(name).inc()

        # Absolute maximum: total number of currently checked out
        # connections reached (base pool_size) + (configured max_overflow),
        # i.e. no further connections can be granted
        if pool.checkedout() == pool.size() + pool._max_overflow:
            DB_POOL_AT_MAX_TOTAL.labels(name).inc()

    @event.listens_for(pool, "checkin")
    def on_checkin(dbapi_connection, connection_record):
        DB_POOL_CHECKEDOUT.labels(name).dec()
        DB_POOL_CHECKINS_TOTAL.labels(name).inc()

    @event.listens_for(pool, "invalidate")
    def on_invalidate(dbapi_connection, connection_record, exception):
        DB_POOL_INVALID_TOTAL.labels(name).inc()

    @event.listens_for(pool, "reset")
    def on_reset(dbapi_connection, connection_record, reset_state):
        DB_POOL_RESET_TOTAL.labels(name).inc()
