# Monitoring example

These are example configuration files for monitoring Tiled with Prometheus and Grafana.

To see a complete working example that incorporates these, clone the tiled
repository and, from the repository root directory, run:

```
TILED_SINGLE_USER_API_KEY=secret docker-compose up
```

**Note that the file `prometheus/prometheus.yml` contains a dummy credential
(`secret`).** To run the example, it must match the secret set by
`TILED_SINGLE_USER_API_KEY`. In real single-user deployments, the secret should
be set to a secure value as described in
[Tiled's security documentation](https://blueskyproject.io/tiled/explanations/security.html).
In multi-user deployments, an
[API key](https://blueskyproject.io/tiled/how-to/api-keys.html) with the
`metrics` scope should be used.
