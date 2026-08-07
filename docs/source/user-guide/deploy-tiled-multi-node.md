# Scale Tiled over Multiple Nodes

Several Scientific User Facilities run deployments of Tiled that are
horizontally scaled over multiple nodes.

At least one uses Kubernetes with a [Helm chart][] maintained in the Tiled repository.
Another uses Ansible for orchestration of a fleet of VMs.

We aim to expand this page to share more details, including host resources,
load balancer configurations, and other recommendations.

## Redis high availability for streaming

Live data streaming (WebSocket subscriptions) is backed by a shared Redis
`streaming_cache`, so in a multi-node deployment every Tiled node must point at
the same Redis. To keep streaming available across a Redis outage, run Redis in
a high-availability topology (a primary with replicas fronted by Redis
Sentinel) and configure Tiled with `sentinels` and `service_name` instead of a
single `uri`; the client then follows Sentinel through a failover and resumes
subscriptions against the newly promoted primary.

Live streaming is **best-effort, not a durable channel**: it rides Redis
Pub/Sub, which is at-most-once, and the durable record is the SQL catalog, which
is committed before the streaming publish. Because Redis replication is
asynchronous and there is no per-write replication confirmation, a Sentinel
failover can drop streamed updates that the old primary acknowledged but had not
yet replicated to the promoted replica (the sequence counter may even rewind).
Setting `min-replicas-to-write` / `min-replicas-max-lag` on the Redis primary
narrows this window but does not close it. See
{doc}`../reference/service-configuration` for the full `streaming_cache` field
reference.

[Helm chart]: https://github.com/bluesky/tiled/pkgs/container/charts%2Ftiled
