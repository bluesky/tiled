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
subscriptions against the newly promoted primary. Because Redis replication is
asynchronous, a failover can drop a streamed write that was acknowledged by the
old primary but not yet replicated. To bound that window, Tiled issues a Redis
`WAIT` write-concern after each publish (`wait_num_replicas`, on by default
under Sentinel); this is best-effort and never fails the client write, so
subscribers needing stronger guarantees should treat delivery as at-least-once
and dedupe on the `sequence` field. See {doc}`../reference/service-configuration`
for the full `streaming_cache` field reference.

[Helm chart]: https://github.com/bluesky/tiled/pkgs/container/charts%2Ftiled
