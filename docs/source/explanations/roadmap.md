# Roadmap

## Path to v0.1.0 Release

We aim to release v0.1.0 in August 2025.

- Support for reading from and writing to blob storage
- Adoption of "closure table" pattern for expressing
  tree structure of nodes with better performance and
  referential integrity
- Experimental support for streaming live data over
  websockets
- A read-only view of array data compatible with Zarr v2,
  suct that Zarr readers can view Tiled as a Zarr store.

## Path to v1.0.0 Release

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
