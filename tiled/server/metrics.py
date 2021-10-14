"""
Read https://prometheus.io/docs/practices/naming/ for naming
conventions for metrics & labels. We generally prefer naming them
`tiled_<noun>_<verb>_<type_suffix>`.
"""

from prometheus_client import Histogram


REQUEST_DURATION = Histogram(
    "tiled_request_duration_seconds",
    "request duration for all HTTP requests",
    ["method", "code"],
)
RESPONSE_SIZE = Histogram(
    "tiled_response_content_length_bytes",
    "response size in bytes for all HTTP requests",
    ["method", "code"],
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
    ["method", "code"],
)
READ_DURATION = Histogram(
    "tiled_read_duration_seconds",
    "time spent reading data for all HTTP requests",
    ["method", "code"],
)
TOKENIZE_DURATION = Histogram(
    "tiled_tokenization_duration_seconds",
    "time spent generating content identifier for all HTTP requests",
    ["method", "code"],
)
PACK_DURATION = Histogram(
    "tiled_pack_duration_seconds",
    "time spent packing (serializing) data for all HTTP requests",
    ["method", "code"],
)
COMPRESSION_DURATION = Histogram(
    "tiled_compress_duration_seconds",
    "time spent applying compression for all HTTP requests",
    ["method", "code", "encoding"],
)
COMPRESSION_RATIO = Histogram(
    "tiled_compress_ratio",
    "compression ratio for all HTTP requests",
    ["method", "code", "encoding"],
    buckets=[1, 2.5, 5, 10, 15, 30, 60, 120, float("inf")],
)

# Initialize labels in advance so that the metrics exist (and can be used in
# dashboards and alerts) even if they have not yet occurred.
for code in ["200", "304", "500"]:
    REQUEST_DURATION.labels(code=code, method="GET")
    RESPONSE_SIZE.labels(code=code, method="GET")
    ACL_DURATION.labels(code=code, method="GET")
    READ_DURATION.labels(code=code, method="GET")
    TOKENIZE_DURATION.labels(code=code, method="GET")
    PACK_DURATION.labels(code=code, method="GET")
    for encoding in ["blosc", "gzip", "lz4", "zstd"]:
        COMPRESSION_DURATION.labels(code=code, encoding=encoding, method="GET")
        COMPRESSION_RATIO.labels(code=code, encoding=encoding, method="GET")


def capture_request_metrics(request, response):
    method = request.method
    code = response.status_code
    metrics = request.state.metrics
    REQUEST_DURATION.labels(method=method, code=code).observe(metrics["app"]["dur"])
    RESPONSE_SIZE.labels(method=method, code=code).observe(
        int(response.headers.get("content-length", 0))
    )
    if "acl" in metrics:
        ACL_DURATION.labels(method=method, code=code).observe(metrics["acl"]["dur"])
    if "read" in metrics:
        READ_DURATION.labels(method=method, code=code).observe(metrics["read"]["dur"])
    if "tok" in metrics:
        TOKENIZE_DURATION.labels(method=method, code=code).observe(
            metrics["tok"]["dur"]
        )
    if "pack" in metrics:
        PACK_DURATION.labels(method=method, code=code).observe(metrics["read"]["pack"])
    if "compress" in metrics:
        encoding = response.headers["content-encoding"]
        COMPRESSION_DURATION.labels(
            method=method, code=code, encoding=encoding
        ).observe(metrics["compress"]["dur"])
        COMPRESSION_RATIO.labels(method=method, code=code, encoding=encoding).observe(
            metrics["compress"]["ratio"]
        )
