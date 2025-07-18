# Roadmap

## Path to v0.1.0 Release

- Support for reading from and writing to blob storage
- Adoption of "closure table" pattern for expressing
  tree structure of nodes with better performance and
  referential integrity
- Experimental support for streaming live data over
  websockets

## Priorities for v0.2.0

- Optimize performance and scalability. Build out nascent
  [asv](https://asv.readthedocs.io/en/latest/) benchmarks, and
  [locust](https://locust.io/) load testing.
- Restructure codebase to separate FastAPI server from
  service, both to improve maintainability/readability and
  to enable a distinct GraphQL server to use the same service.
- Rationalize and simplify CLI and configuration management.
- Support deployment with an API gateway.
- Continue to develop streaming capabilities.
