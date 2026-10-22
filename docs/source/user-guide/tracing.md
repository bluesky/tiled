# Distributed Tracing

In addition to [Prometheus metrics](./metrics.md), Tiled can emit
[OpenTelemetry](https://opentelemetry.io/) traces. Whereas metrics describe
aggregate behavior across many requests, a *trace* records the timeline of a
single request as a tree of *spans*. This is useful for investigating why a
particular request was slow.

Traces are exported using the OpenTelemetry Protocol (OTLP) to an
[OpenTelemetry Collector](https://opentelemetry.io/docs/collector/), which
forwards them to one or more tracing backends for storage and visualization,
such as [Jaeger](https://www.jaegertracing.io/) or
[Grafana Tempo](https://grafana.com/oss/tempo/).

```{mermaid}
flowchart LR
    tiled["Tiled server"]
    collector["OpenTelemetry<br/>Collector"]

    subgraph backends["Storage backends"]
        direction TB
        jaeger["Jaeger"]
        tempo["Grafana Tempo"]
        prometheus["Prometheus"]
        loki["Loki"]
    end

    subgraph viz["Visualization"]
        direction TB
        jaegerui["Jaeger UI"]
        grafana["Grafana"]
    end

    %% Configured in the example
    tiled -->|"traces (OTLP)"| collector
    collector -->|OTLP| jaeger
    collector -->|OTLP| tempo
    tiled -->|"metrics (scrape)"| prometheus

    %% Visualization
    jaeger --> jaegerui
    tempo --> grafana
    prometheus --> grafana

    %% Metrics and logs over OTLP: possible extension, not enabled
    tiled -.->|"metrics (OTLP)"| collector
    tiled -.->|"logs (OTLP)"| collector
    collector -.->|metrics| prometheus
    collector -.->|logs| loki
    loki -.-> grafana
```

Solid arrows are what the example configures today: Tiled pushes **traces** over
OTLP to the Collector, which fans them out to Jaeger and Grafana Tempo, while
Prometheus scrapes Tiled's metrics endpoint. Dashed arrows show how the same
Collector could also carry OpenTelemetry's other two signals — **metrics** and
**logs** — over OTLP to backends such as Prometheus and Loki. Those paths are
not currently enabled.

## Enabling tracing

Tracing is **disabled by default**. It is turned on by setting the standard
OpenTelemetry environment variable `OTEL_EXPORTER_OTLP_ENDPOINT` to the address
of an OTLP endpoint (an OpenTelemetry Collector, or a backend that accepts OTLP
directly). Related environment variables:

| Variable | Purpose |
| --- | --- |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP endpoint, e.g. `http://otel-collector:4318`. Tracing is off when this is unset. |
| `OTEL_SERVICE_NAME` | Name shown for the service in the tracing backend, e.g. `tiled`. |
| `OTEL_PYTHON_FASTAPI_EXCLUDED_URLS` | Comma-separated URL patterns to exclude from tracing, e.g. `healthz,api/v1/metrics` to skip health checks and metrics scrapes. |


## How does it work?

1. When `OTEL_EXPORTER_OTLP_ENDPOINT` is set, Tiled configures an OpenTelemetry
   tracer and instruments the ASGI application, creating one span per incoming
   HTTP request.

2. Spans are exported over OTLP to the OpenTelemetry Collector.

3. The Collector forwards traces to one or more backends (Jaeger and Grafana
   Tempo in the example stack), which store them and make them available to
   search and visualize.


## Finding the trace of a request

Each traced response carries the request's trace ID (32 hexadecimal digits) in
the `X-Tiled-Trace-ID` header, next to the `X-Tiled-Request-ID` correlation ID.
The server's log lines for the request show both IDs, for example:

```
[3f2a9c1e04b7d816 0af7651916cd43dd8448eb211c80319c] 127.0.0.1:52344 (singleuser) - "GET /api/v1/metadata/ HTTP/1.1" 200 OK
```

To open a trace, paste its ID into the search box at the top of the Jaeger UI,
or into a TraceQL query in Grafana's Explore view (Tempo data source).

If you only have the correlation ID (for example, from a client error message
for a `4xx` response, or from logs that do not include the trace ID), search
for it instead: it is recorded in the request's span as the `tiled.request_id`
attribute. In the Jaeger UI, enter `tiled.request_id=3f2a9c1e04b7d816` under
**Tags**; in Grafana, use the TraceQL query
`{ span.tiled.request_id = "3f2a9c1e04b7d816" }`.

### When a request fails

When the server returns an error, the Python client includes both IDs in the
exception message, for example:

```
Server error '500 Internal Server Error' for url 'http://localhost:8000/api/v1/...'
For more information, server admin can search server logs for correlation ID 3f2a9c1e04b7d816 and traces for trace ID 0af7651916cd43dd8448eb211c80319c.
```

### When a request is slow

Record the client's requests while running the slow operation, then print how
long each took and its trace ID:

```python
from tiled.client import record_history

with record_history() as history:
    ...  # the slow operation, e.g. c["some/array"].read()

for response in history.responses:
    print(response.elapsed, response.request.url, response.headers.get("x-tiled-trace-id"))
```

Alternatively, `tiled.client.show_logs()` logs every request and response,
including its headers, with timestamps.

Without a trace ID at hand, search the backend for slow requests instead: in
the Jaeger UI, set **Min Duration** (e.g. `1s`) when searching the **tiled**
service; in Grafana, use a TraceQL query such as
`{ resource.service.name = "tiled" && kind = server && duration > 1s }`.

### When there is no trace ID

The header and the trace ID in the logs are omitted when there is no trace to
look up:

- tracing is disabled on the server;
- the URL is excluded from tracing (`OTEL_PYTHON_FASTAPI_EXCLUDED_URLS`);
- the request is not sampled, i.e. the trace is deliberately not recorded.

A client that is itself traced (for example, another service instrumented with
OpenTelemetry) sends its own trace ID with each request, in the standard
[`traceparent`](https://www.w3.org/TR/trace-context/#traceparent-header) header.
Tiled then adds its spans to the client's trace instead of starting a new one,
so the response carries the client's trace ID, and the trace shows the work on
both sides. Whether such a request is sampled is decided by the client. Ordinary
clients, including the Tiled Python client, do not send this header.


## Try it with the example stack

Tiled ships example configuration that runs an OpenTelemetry Collector and
Jaeger alongside the server, Prometheus, and Grafana. From the repository root,
start the server together with the monitoring services:

```
TILED_SINGLE_USER_API_KEY=secret \
  docker compose -f compose.dev.yml -f compose.monitoring.yml up --build
```

`compose.dev.yml` builds the Tiled image from this checkout (so it includes the
tracing support), and `compose.monitoring.yml` sets the `OTEL_*` variables above
and runs the Collector, so the server exports traces to it. (The published image
referenced by `compose.yml` may not yet include tracing.)

Generate some activity using the Tiled Python client:

```python
from tiled.client import from_uri

c = from_uri("http://localhost:8000", api_key="secret")
c.create_container('test')
list(c)
```

The example forwards traces to two backends so you can compare their functionality:

- **Jaeger:** open [http://localhost:16686](http://localhost:16686), select the
  **tiled** service, and click **Find Traces**. Click a trace to see its span
  waterfall.
- **Grafana Tempo:** open [http://localhost:3000](http://localhost:3000), go to
  **Explore**, select the **Tempo** data source, and search using
  [TraceQL](https://grafana.com/docs/tempo/latest/traceql/), for example
  `{ resource.service.name = "tiled" }`.

```{note}
The bundled Collector also scrapes Tiled's `/api/v1/metrics` endpoint and
re-exposes it on port 8889, in addition to Prometheus scraping it directly.
To disable this, remove the `metrics` pipeline from the Collector
configuration in `monitoring_example/otel-collector/otel-collector.yml`.
See [Prometheus Metrics](./metrics.md).
```
