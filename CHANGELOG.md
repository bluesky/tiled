<!-- Add the recent changes in the code under the relevant category.
Write the date in place of the "Unreleased" in the case a new version is released. -->

# Changelog

## Unreleased

### Fixed

- The `content-encoding` `blosc` was recently upgraded from Blosc to Blosc2.
  The `content-encoding` has been renamed to `blosc2` to avoid version
  confusion between different versions of Tiled servers and clients.

## v0.1.0a119 (24 April 2024)

### Fixed

- Metric-reporting had an error when `compress` timing was recorded but
  `content-encoding` header was unset.

## v0.1.0a118 (23 April 2024)

### Added

- Python 3.12 support
- Added `tiled.adapters.resource_cache` for caching file handles between
  requests.

### Removed

- Removed object cache from the codebase. If `object_cache` is included in
  the server configuration file, a warning is raised that this configuration
  has no effected.

### Fixed

- The configuration setting `tiled_admins` did not work in practice. If a user
  in the list was already an admin (such as, after a server restart) an error
  was raised on startup.
- The table creation statements for PostgreSQL were not committed. (This may
  have been a regression due to a change in SQLAlchemy defaults.)
- Tolerate HTTP responses that are missing a `x-tiled-request-id` header, such
  as when a proxy server responds with an error.
- Use `httpx` status code sentinels (instead of `starlette` ones) for typing
  client-side status codes.

### Changed

- Removed upper bound version pin on `dask`.
- Switched from `blosc` to `blosc2`.
- Made client objects dask-serializable
- Added support for registering multiple Assets with a DataSource in an update


### Other

- Usage of deprecated Pydantic 2.x APIs was updated.
- Specify a `fallback-version`, `0.0.0`, to be used when the version-detection
  code cannot run.

## v0.1.0a117 (28 March 2024)

### Added

- Support for specifying the format that uploaded data will be stored in.
- Support for storing uploaded tabular data in CSV format.
- A new HTTP endpoint, `PATCH /api/v1/table/partition/{path}`
  supporting appending rows to a tabular dataset.
- A new method `DataFrameClient.append_partition`.
- Support for registering Groups and Datasets _within_ an HDF5 file
- Tiled version is logged by server at startup.

### Fixed

- Critical regression that broke `tiled serve directory ...` CLI.

### Other

 - Updated the pydantic version in the pyproject.toml. Now the allowed versions are >2.0.0 - <3.0.0 .
 - Changes to prepare for upcoming numpy 2.0 release
 - Changes to address deprecations in FastAPI
