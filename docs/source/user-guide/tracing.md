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
