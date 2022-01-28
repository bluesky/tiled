"""
Read https://prometheus.io/docs/practices/naming/ for naming
conventions for metrics & labels. We generally prefer naming them
`tiled_<noun>_<verb>_<type_suffix>`.
"""

from functools import lru_cache

from fastapi import APIRouter, Request, Response, Security
from prometheus_client import Histogram

from .authentication import get_current_principal

router = APIRouter()

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
        for encoding in ["blosc", "gzip", "lz4", "zstd"]:
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
        encoding = response.headers["content-encoding"]
        COMPRESSION_DURATION.labels(
            method=method, code=code, endpoint=endpoint, encoding=encoding
        ).observe(metrics["compress"]["dur"])
        COMPRESSION_RATIO.labels(
            method=method, code=code, endpoint=endpoint, encoding=encoding
        ).observe(metrics["compress"]["ratio"])


@lru_cache()
def prometheus_registry():
    """
    Configure prometheus_client.

    This is run the first time the /metrics endpoint is used.
    """
    # The multiprocess configuration makes it compatible with gunicorn.
    # https://github.com/prometheus/client_python/#multiprocess-mode-eg-gunicorn
    from prometheus_client import CollectorRegistry
    from prometheus_client.multiprocess import MultiProcessCollector

    registry = CollectorRegistry()
    MultiProcessCollector(registry)  # This has a side effect, apparently.
    return registry


@router.get("/metrics")
async def metrics(
    request: Request, principal: Security(get_current_principal, scopes=["metrics"])
):
    """
    Prometheus metrics
    """
    from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

    request.state.endpoint = "metrics"
    data = generate_latest(prometheus_registry())
    return Response(data, headers={"Content-Type": CONTENT_TYPE_LATEST})
