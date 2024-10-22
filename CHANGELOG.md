<!-- Add the recent changes in the code under the relevant category.
Write the date in place of the "Unreleased" in the case a new version is released. -->

# Changelog

## v0.1.0b10 (2024-10-11)

- Add kwarg to client logout to auto-clear default identity.
- Do not automatically enter username if default identity is used.
- Add API route and Python client method enabling admins to
  reokve API keys belonging to any user or service.

## v0.1.0b9 (2024-09-19)

### Added

- Added support for explicit units in numpy datetime64 dtypes.

### Fixed

- Follow-up fix to compatibility with Starlette v0.38.0
- Adapt to change in dask public API in dask 2024.9.0.

## v0.1.0b8 (2024-09-06)

### Fixed

- Compatibility with a change in Starlette v0.38.0

## v0.1.0b7 (2024-08-20)

### Added

- Add method to `TableAdapter` which accepts a Python dictionary.
- Added an `Arrow` adapter which supports reading/writing arrow tables via `RecordBatchFileReader`/`RecordBatchFileWriter`.

### Changed

- Make `tiled.client` accept a Python dictionary when fed to `write_dataframe()`.
- The `generated_minimal` example no longer requires pandas and instead uses a Python dict.
- Remove unused pytest-warning ignores from `test_writing.py`.
- Rename argument in `hdf5_lookup` function from `path` to `dataset` to reflect change in `ophyd_async`

## v0.1.0b6 (2024-07-17)

### Added

- A `/healthz` endpoint, for use by orchestration infrastructure

### Fixed

- A bug in `Context.__getstate__` caused picking to fail if applied twice.

## v0.1.0b5 (2024-06-27)

### Added
- Support partial download of an asset using the
  [HTTP `Range` Header](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Range).

### Fixed
- When authenticated as a Service Principal, display the SP's uuid in
  the client Context repr.
- The dependency `json-merge-patch`, introduced in v0.1.0b4, was missing from
  some pip selectors.

## v0.1.0b4 (2024-06-18)

### Changed

- Minor implementation changes were necessary to make Tiled compatible with
  Numpy 2.0.
- For improved security, the server-side array slicing function has been
  refactored to avoid using `eval()`. To be clear: there were no known
  exploitable vulnerabilities in the `eval()` approach. The input was validated
  against a regular expression before being passed to `eval()`. However,
  avoiding `eval()` altogether is better practice for defense-in-depth against
  potential code injection attacks due to current or future bugs in Tiled or
  its upstream dependencies.

## v0.1.0b3 (2024-06-04)

### Added

- Added a new HTTP endpoint, `PATCH /api/v1/metadata/{path}` supporting modifying
  existing metadata using a `application/json-patch+json` or a
  `application/merge-patch+json` patch.
- Added client-side methods for replacing, updating (similar to `dict.update()`),
  and patching metadata.

### Fixed

- Fixed regression introduced in the previous release (v0.1.0b1) where exceptions
  raised in the server sent _no_ response instead of properly sending a 500
  response. (This presents in the client as, "Server disconnected without
  sending a response.") A test now protects against this class of regression.

## v0.1.0b2 (2024-05-28)

### Changed

- Customized default logging configuration to include correlation ID and username
  of authenticated user.
- Added `--log-timestamps` CLI flag to `tiled serve ...` to opt in to including
  timestamp prefix in log messages.

## v0.1.0b1 (2024-05-25)

### Added
- Support for `FullText` search on SQLite-backed catalogs

### Fixed

- Updated `BaseClient.formats` to use the `dict` structure for specs.
- The `tiled serve directory --watch` function was not compatible with recent `anyio`

## v0.1.0a122 (23 May 2024)

### Fixed

- A dependency on `fastapi` was introduced in `tiled.adapters`. This has been
  removed.

## v0.1.0a121 (21 May 2024)

### Added

- The `tiled serve ...` CLI commands now accept a `--log-config` option,
  pointing to a custom uvicorn logging configuration file. An example
  file was added to the repository root, `example_log_config.yml`.
- Added `tiled.adapters.protocols` which will provide possibility for user to
  implement their custom adapters in a way that satisfies mypy.
- Added `tiled.client.smoke` with a utility for walking a node and ensuring
  that the data in it can be read.
- Added `tiled.client.sync` with a utility for copying nodes between two
  Tiled instances.
- Show authentication state in `Context` repr.

### Changed

- SQLite-backed catalogs now employ connection pooling. This results in a
  significant speed-up and avoids frequently re-opening the SQLite file.
- Metadata returned from the use of the `select_metadata` is now a one-item
  dictionary with 'selected' as the key, to match default type/behavior.
- The method `BaseClient.data_sources()` returns dataclass objects instead of
  raw dict objects.
- `tiled.client.sync` has conflict handling, with initial options of 'error'
  (default), 'warn', and 'skip'

### Fixed

- Propagate setting `include_data_sources` into child nodes.
- Populate attributes in member data variables and coordinates of xarray Datasets.
- Update dependencies.
- Fix behavior of queries `In` and `NotIn` when passed an empty list of values.

## v0.1.0a120 (25 April 2024)

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
