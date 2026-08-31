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

Live streaming is **best-effort**: it is delivered over Redis Pub/Sub, which is
at-most-once, and the durable record is the SQL catalog, which is committed
before the streaming publish. An ordinary connection drop is
lossless — the client reconnects and resumes from the sequence number after the
last one it received, and the server replays anything it missed — so a normal
reconnect neither loses nor duplicates updates.

The residual risk is a Sentinel failover to a **replica that had not yet caught
up**. Redis replication is asynchronous and there is no per-write replication
confirmation, so two things can happen. First, any update the old primary
accepted but had not yet copied to that replica is gone — subscribers never
receive it. Second, the promoted replica's sequence counter is lower than the
old primary's, so as new updates arrive it hands out sequence numbers that were
already used, and subscribers receive those numbers a second time (now labeling
different data). Setting `min-replicas-to-write` / `min-replicas-max-lag` on the
Redis primary narrows this window but does not close it. See
{doc}`../reference/service-configuration` for the full `streaming_cache` field
reference.

## Postgres high availability

Tiled stores its data in Postgres. To keep Tiled writable across a database
node outage, run Postgres in a high-availability topology (a primary with
streaming replicas, managed by a tool such as [Patroni][] for automatic
failover) and give Tiled a **multi-host connection URI** listing every cluster
member, with `target_session_attrs=read-write` so the client connects to
whichever node is currently the read-write primary. The hosts are specified
using SQLAlchemy's
[multiple host connection string syntax][]:

```yaml
database:
  uri: "postgresql://tiled:${TILED_DATABASE_PASSWORD}@/tiled?host=db1.example:5432&host=db2.example:5432&host=db3.example:5432&target_session_attrs=read-write&ssl=verify-full"
```

[Helm chart]: https://github.com/bluesky/tiled/pkgs/container/charts%2Ftiled

[Patroni]: https://patroni.readthedocs.io/

[multiple host connection string syntax]: https://docs.sqlalchemy.org/en/20/dialects/postgresql.html#multihost-connections
