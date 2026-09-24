# Distributed Tracing

In addition to [Prometheus metrics](./metrics.md), Tiled can emit
[OpenTelemetry](https://opentelemetry.io/) traces. Whereas metrics describe
aggregate behavior across many requests, a *trace* records the timeline of a
single request as a tree of *spans*. This is useful for investigating why a
particular request was slow.

Traces are exported using the OpenTelemetry Protocol (OTLP) to an
[OpenTelemetry Collector](https://opentelemetry.io/docs/collector/), which
forwards them to a tracing backend such as
[Jaeger](https://www.jaegertracing.io/) for storage and visualization.

```
tiled  --OTLP-->  OpenTelemetry Collector  --OTLP-->  Jaeger
```

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

3. The Collector forwards traces to Jaeger, which stores them and serves the UI
   used to search and visualize them.


## Try it with the example stack

Tiled ships example configuration that runs an OpenTelemetry Collector and
Jaeger alongside the server, Prometheus, and Grafana. From the repository root,
start the server together with the monitoring services:

```
TILED_SINGLE_USER_API_KEY=secret \
  docker compose -f compose.yml -f compose.monitoring.yml up
```

The `compose.yml` file already sets the `OTEL_*` variables above so that the
server exports traces to the bundled Collector.

Generate some activity using the Tiled Python client:

```python
from tiled.client import from_uri

c = from_uri("http://localhost:8000", api_key="secret")
c.create_container('test')
list(c)
```

Then open the Jaeger UI at
[http://localhost:16686](http://localhost:16686), select the **tiled** service,
and click **Find Traces**. Click any trace to see its span waterfall.

```{note}
The bundled Collector also scrapes Tiled's `/api/v1/metrics` endpoint and
re-exposes it on port 8889, in addition to Prometheus scraping it directly.
To disable this, remove the `metrics` pipeline from the Collector
configuration in `monitoring_example/otel-collector/otel-collector.yml`.
See [Prometheus Metrics](./metrics.md).
```
