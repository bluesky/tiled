# Roadmap

## Path to v1.0.0 Release

We aim to release v1.0.0 in December 2025. There may or may
not be a v0.2.0 release along the way.

- Optimize performance and scalability. Build out nascent
  [asv](https://asv.readthedocs.io/en/latest/) benchmarks, and
  [locust](https://locust.io/) load testing.
- Restructure codebase to separate FastAPI server from
  service, both to improve maintainability/readability and
  to enable a distinct GraphQL server to use the same service.
- Type-hint more of the codebase.
- Rationalize and simplify CLI and configuration management.
- Support deployment with an API gateway.
- Continue to develop streaming capabilities.
- Support for reading from and writing to blob storage
