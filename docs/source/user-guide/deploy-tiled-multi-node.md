# Scale Tiled over Multiple Nodes

Several Scientific User Facilities run deployments of Tiled that are
horizontally scaled over multiple nodes.

At least one uses Kubernetes with a [Helm chart][] maintained in the Tiled repository.
Another uses Ansible for orchestration of a fleet of VMs.

We aim to expand this page to share more details, including host resources,
load balancer configurations, and other recommendations.

[Helm chart]: https://github.com/bluesky/tiled/pkgs/container/charts%2Ftiled
