<!-- Add the recent changes in the code under the relevant category.
Write the date in place of the "Unreleased" in the case a new version is released. -->

# Changelog

## v0.2.3 (2025-12-17)

### Added

- Add ExternalPolicyDecisionPoint for authorization and an example with Open Policy Agent
- Subscriptions retry connecting if the websocket connection is interrupted.
- React UI supports server-side sorting.

### Changed

- Rename "create" scope to the more explicit "create:node"
- Split "apikeys" scope into "create:apikeys" and "revoke:apikeys" scopes

### Fixed

- Slicing on a CompositeClient previous always returned the full results.
  Now it slices as expected.
- Made provision for forks of the repository to publish Helm charts.
- Allow clients to register standalone data directories as single nodes
  (e.g. Zarr stores) directly rather than discovering them by walking their
  parent directory.
- Fix regression that broke registering files at a prefix.
- Servers that allow anonymous access (i.e. `--public`) allow streaming
  subscribers to connect without authentication.

## v0.2.2 (2025-11-25)

### Changed

- Deletion of nodes or metadata revisions now requires deletion scopes.

### Fixed

- In-memory SQLite databases are connection pooled / cached.
- Addressed backward-incompatible changes in dependencies `fastapi` and `minio`.
  rather than writing scopes.
- Pinned down pydantic-settings until breaking changes can be addressed.
- Writing I/O calls in the Zarr adapter were blocking the server event loop;
  they are now properly on a thread.

## Fixed

- Fixed a couple of bugs in the example config, to restore it to working order


## v0.2.1 (2025-11-12)

### Added

- Optional `persist` query parameter to PUT and PATCH /array/... routes, and
  the corresponding DaskArrayClient methods: `write`, `write_block`, `patch`.
- Added new delete:node and delete:revision scopes

### Changed

- The public demo hosted by NSLS2 has moved from `tiled-demo.blueskyproject.io`
  to `tiled-demo.nsls2.bnl.gov`, for purely practical reasons. (It is easier to
  manage the deployment and associated certificates.) **The demo remains
  world-public, with no login required.** This change affects some
  documentation and one test.

## v0.2.0 (2025-10-29)

### Added

- Tests to ensure that CSVAdapter can be used with a subset of columns.
- `locking` key-word argument in HDFAdapter and HDF5Adapter.
- Support for streaming uploaded tabular data.

### Changed

- Enable Tiled server to accept bearer access tokens for authentication.
- Modernize implementation of Device Code Flow.
- The websocket messages contain a `"type"` field and are not formally
  specified and validated using pydantic models in `tiled.stream_messages`.
- In previous releases, the method `Subscription.start` launched a background
  thread. This was renamed to `Subscription.start_in_thread`. Now
  `Subscription.start` blocks the current thread, leaving any thread management
  up to the caller.
- Encapsulated redis code in adapter.py into a StreamingCache object
- Renamed `Subscription.stop` to `Subscription.disconnect`.
- In `Subscription`, use a configurable `concurrent.futures.Executor` to
  execute callbacks.
- Removed `Subscription.add_callback`, replaced by separate callback registries
  connected to specific types of updates:

  ```python
  Subscription.stream_closed  # writer-initiated
  Subscription.disconnected  # subscriber-initiated
  ContainerSubscription.child_created
  ContainerSubscription.child_metadata_updated
  ArraySubscription.new_data
  TableSubscription.new_data
  ```

- The signature of subscription callbacks has been changed. The
  connection-related callbacks `stream_closed` and `disconnected`
  receive `f(subscription: Subscription)`. The other callbacks
  receive an instance of `tiled.client.stream.LiveUpdate`,
  which provide a reference to the subscription
  `update.subscription`, all the data from the websocket message,
  and update-specific convenience methods such as
  `LiveContainerUpdate.child()` when a new child is created in a
  container and `ArraySubscription.new_data()` or
  `TableSubscription.new_data()` to conveniently access an array
  or table respectively.

### Fixed

- Prevent exception when serving asset from a node if stat_result already found
- Column names in `TableStructure` are explicitly converted to strings.
- Ensure that structural dtype arrays read with `CSVAdapter` have two dimensions, `(n, 1)`.
- Updated minimum version of starlette, which implements new (standard) names
  for HTTP status codes
- Allow extra kwargs to be passed to `HDF5ArrayAdapter` when intialized via `HDF5Adapter`
  with an explicit `dataset` parameter.
- Prevent exception when serving asset from a node if stat_result already found
- Fix bug in reading dask-backed `CompositeClient`.

### Refactored

- Use common base type for all access policy types


## v0.1.6 (2025-09-29)

### Fixed

- Resolved circular dependency in `tiled.storage`.


## v0.1.5 (2025-09-26)

### Added

- Monitoring of pool overflow and max-out events.

### Fixed

- Additional kwargs (`include_data_sources`, `queries`, `sorting`) propagated to
  the instantiated container when calling `CompositeClient.base`.
- Fix AuthN database connection lifecycle management. Connections were being
  held for the duration of the request cycle; now they are released immediately
  after use.


## v0.1.4 (2025-09-24)

### Fixed

- A regression in v0.1.1 broke the ability of adapters to add custom endpoints
  on the server, which is used by legacy databroker to add a `/documents`
  endpoint.


## v0.1.3 (2025-09-24)

### Added

- Monitoring of the number of connections in the database pools.

### Changed

- Implemented a process-global connection pool for catalog databases, similarly
  to the connections to authN database.

### Fixed

- A regression in v0.1.1 disallowed specifying `allow_origins`, `specs`, and `trees`
  across multiple files. (This can be useful for config.d-style configuration
  where different config files are managed by different stages of configuration
  management.)
- A regression in v0.1.1 disallowed specifying
  `tree: databroker.mongo_normalized:MongoAdapter.from_uri` or in fact
  specifying any method (e.g. classmethod constructor) as a tree.


## Unreleased

### Changed

- `Subscription.add_callback` and `Subscription.remove_callback` now
  return the `Subscription` instance, enabling this:

  ```py
  sub.add_callback(f)
  sub.add_callback(g)
  sub.start()
  ```

  to be written as:

  ```py
  sub.add_callback(f).add_callback(g).start()
  ```


## v0.1.2 (2025-09-17)

This release temporarily pins backs the version of the depedency DuckDB to
avoid breakage (seemingly unintended) to documented behavior.


## v0.1.1 (2025-09-10)

### Fixed

- In the Tiled container image, the React UI was misplaced and thus
  did not function.
- The Tiled container image was missing some client-side dependencies needed to
  run the `tiled serve directory ...` command.

### Changed

- Internally, pydantic is used to parse configuration.


## v0.1.0 (2025-09-04)


### Added

- Convenience method `subscribe` on client `Container` and `ArrayClient`
  returns an experimental `Subscription`. See the new Streaming tutorial
  for usage.

### Changed

- Client method for writing tabular data into external files, `write_dataframe`,
  is deprecated and renamed `write_table`.
- The order of arguments in the `write_partition` and `append_partition` methods.
- The experiment `Subscription` object now takes where to start as an argument
  to `Subscription.start` instead of at initialization time.

### Fixed

- Handling for certain catalog edge cases when building the nodes_closure table.
- Enforce validity checks when adding appendable tables to "composite"-spec'ed containers.

## v0.1.0-b39 (2025-08-28)

### Fixed

- Default paraneter (`None`) for the `patch` parameter in `PUT /data_source` endpoint.


## v0.1.0-b38 (2025-08-28)

### Fixed

- Critical bug in new `tiled.access_control` code, missing `__init__.py`.


## v0.1.0-b37 (2025-08-28)

### Added

- The access tags compiler and db schema have been upstreamed into Tiled
- API keys can now be restricted to specific access tags
- New unit tests covering the new access policy and access control features
- Experimental support for streaming array data over a websocket endpoint.
  Documentation to follow.

### Changed

- Remove `SpecialUsers` principals for single-user and anonymous-access cases
- Access control code is now in the `access_control` subdirectory
- `SimpleAccessPolicy` has been removed
- AuthN database can now be in-memory SQLite
- Catalog database can now be shared when using in-memory SQLite
- `TagBasedAccessPolicy` now supports anonymous access
- `AccessTagsParser` is now async
- `toy_authentication` example config now uses `TagBasedAccessPolicy`
- Added helpers for setting up the access tag and catalog databases for `toy_authentication`

### Fixed

- Access control on container export was partially broken, now access works as expected.


## v0.1.0-b36 (2025-08-26)

### Changed

- Demoted the `Composite` structure family to `composite` spec.
- Typehint utils collection implementations


## v0.1.0-b35 (2025-08-20)

### Changed

- Optimized the calculation of an approximate length of containers.

### Added

- The project ships with a pixi manifest (`pixi.toml`).
- Connection pool settings for catalog and storage databases.

## v0.1.0-b34 (2025-08-14)

### Fixed

- In the previous release, v0.1.0-b32, a catalog database migration script (for
  closure tables) ran successfully on some databases but on others it could
  fail. As designed, the failure mode was a clean rollback, leaving the
  database correct but unchanged. This release repairs the migration script; it
  should be re-run on any databases that could not be upgraded with the previous
  release.

## v0.1.0-b33 (2025-08-13)

_This release requires a database migration of the catalog database._

```none
tiled catalog upgrade-database [postgresql://.. | sqlite:///...]
```

### Added

- Endpoints for (read) data access with zarr v2 and v3 protocols.
- `data_type` and `coord_data_type` properties for sparse arrays in `COOAdapter`
  and `COOStructure`.

### Changed

- Refactored internal server function ``get_root_tree()`` to not use FastAPI
  dependencies injection
- The logic of hierarchical organization of the Nodes table in Catalog: use the concept
  of Closure Table to track ancestors and descendands of the nodes.
- Shorter string representation of chunks in `ArrayClient`.
- Refactored internal Zarr version detection
- For compatibility with older clients, do not require metadata updates to include
  an `access_blob` in the body of the request.

### Fixed

- Uniform array columns read from Postgres/DuckDB are now aggregated to an
  NDArray (e.g. scanned `waveform` PVs)
- Support for deleting separate nodes and contents of containers in client API.
- The database migration in v0.1.0-b27 was incomplete, and missed an update to
  the `revisions` table necessary to make metadata updates work correctly.
  This is resolved by an additional database migration.
- Correct indentation of authenticator args field in the service config schema
  and ensure it correctly validates configurations.


## v0.1.0-b32 (2025-08-04)

This release is identical to the previous one; it was made to fix our
continuous deployment processes.


## v0.1.0-b31 (2025-08-01)

### Added

- Pooling of ADBC connections to storage databases.
- An index on the `node_id` column of the `data_sources` table.

### Fixed

- Make devcontainer work out of box to run e.g. tiled serve demo
- Tests were missing assertions to verify expected outcomes
- Combining multiple hdf5 files containing scalar values by HDF5Adapter.
- Make principal type hints consistent in router
- Typehinted database access methods
- Explicit type conversion in SQL adapter when appending to an existing table.

## v0.1.0-b30 (2025-07-18)

### Changed

- Refactored internal server function ``get_entry()`` to not use the FastAPI
  dependencies injection
- Updated front-end dependencies, and updated node version used for building
  front-end.

### Fixed

- Restored authentication check for API key
- Updated usage for change in Zarr 3.x API.
- Improved error message if config location is non-file


## v0.1.0-b29 (2025-06-06)

### Added

- It is now possible to explicitly control the page size used for fetching
  batches of metadata, e.g. `client.values().page_size(N)`.
- Writable tabular SQL storage in SimpleTiledServer.
- The `pyproject.toml` now includes integration with [pixi](https://pixi.sh/).

### Fixed

- An auth bug that prevented a user to create a table with empty access_tags.
- When accessing a small number of results, the page size is set appropriately
  to avoid needlessly downloading additional results.
- The `tiled serve config ...` CLI command silently ignored `--port 0` and
  used the default port (`8000`).

## v0.1.0-b28 (2025-05-21)

### Changed

- Accept (allowed) special characters in SQL column names, e.g. "-".
- The large `TagBasedAccessPolicy` class introduced in the previous release
  was refactored into separate objects.

## 0.1.0-b27 (2025-05-08)

_This release requires a database migration of the catalog database._

```none
tiled catalog upgrade-database [postgresql://.. | sqlite:///...]
```

### Added

- New access policy `TagBasedAccessPolicy` which introduces more robust
  authorization based on the concept of tagging. When this policy is used,
  access to data is controlled by the node's `access_blob` (i.e the tags applied
  to that node).
- Added new `access_blob` column to catalog database, in support of the new
  authorization. This blob typically contains one of: resource owner (creator),
  or a list of access tags.
- Added new filter type `AccessBlobFilter` which filters nodes based upon their
  `access_blob` contents. In support of the new authorization.

### Changed
- Tiled now accepts a single `access_control` configuraton for the entire
  server, only. Access policies are now a server-wide singleton used for
  all access requests. Access control can no longer be specified on
  individual trees.
- Removed `path_parts` arg from access policy signatures and related.
- Effective scopes for the principal (from authN) are now threaded into
  access policies and related.
- Removed `access_policy` from `MapAdapter` and `CatalogAdapter`; accesss policies
  are now set server-wide only.

## 0.1.0-b26 (2025-05-07)

### Added

- New query parameter `drop_revision` on endpoints `PUT /metadata/{path}`
  and `PATCH /metadata/{path}`. If set to true, the version replaced by
  the update is _not_ saved as a revision. This is exposed in the Python
  client via a new keyword-only argument `drop_revision` in
  `update_metadata`, `patch_metadata`, and `replace_metadata`.

### Fixed

- A critical bug in the `mount_node` feature introduced in the
  previous release prohibited the server from starting when
  `mount_node` was used with a PostgreSQL database.
- Accept (allowed) special characters in SQL column names, e.g. "-".

## 0.1.0-b25 (2025-05-06)

### Added

- New optional parameter to catalog configuration, `mount_node`
  enables mounting different sub-trees of one catalog database
  at different prefixes. This is an advanced feature to facilitate
  migration from many catalogs to one. See
  `tiled/_tests/test_mount_node.py` for usage.

## 0.1.0-b24 (2025-05-06)

### Added

- Support for reading numpy's on-disk format, `.npy` files.

### Changed

- In server configuration, `writable_storage` now takes a list of URIs,
  given in order of decreasing priority.
- Adapters should implement a `supported_storage` attribute, as specified
  in `tiled.adapters.protocols.BaseAdapter`. This is optional, for
  backward-compatiblity with existing Adapters, which are assumed to
  use file-based storage.

### Fixed

- When using SQL-backed storage and file-backed storage, Tiled treated SQLite
  or DuckDB files as if they were directories of readable files, and
  included them superfluously in a check on whether assets were situated
  in a readable area.
- Update data_sources in the client after receiving a response from the server.
  Removed the (unused) `data_source` parameter from the `PUT /data_source/`
  endpoint; the id of the updated data source must be included in the structure
  within the body of the request.

## 0.1.0-b23 (2025-04-24)

### Added

- New query type `Like` enables partial string match using SQL `LIKE`
  condition.

### Changed

- Exposed `Session.state` information from database to enhance custom access
  control developments.

## 0.1.0-b22 (2025-04-21)

### Added

- Tiled now retries HTTP requests that fail due to server-side (`5XX`) or
  connection-level problems.
- Support for `async` streaming serializers (exporters)

### Changed

- Iteration over a `Composite` client yields its (flattened) keys, not its
  internal parts. This makes `__iter__` and `__getitem__` consistent.

## 0.1.0-b21 (2025-04-15)

### Added

- `Composite` structure family to enable direct access to table columns in a single namespace.
- Added a `parent` property to BaseClass that returns a client pointing to the parent node
- New CLI flag `tiled --version` shows the current version and exits.

### Changed

- Adjust arguments of `print_admin_api_key_if_generated` and rename `print_server_info`
- Allow `SQLAdapter.append_partition` to accept `pyarrow.Table` as its argument
- Fix streaming serialization of tables keeping the dtypes of individual columns

### Maintenance

- Extract API key handling
- Extract scope fetching and checking
- Refactor router construction
- Adjust environment loading
  - This is a breaking change if setting `TILED_SERVER_SECRET_KEYS` or
    `TILED_ALLOW_ORIGINS`. `TILED_SERVER_SECRET_KEYS` is now
    `TILED_SECRET_KEYS` and these fields now require passing a json
    list e.g. ``TILED_SECRET_KEYS='["one", "two"]'``
- More type hinting
- Refactor authentication router construction
- Set minimum version of FastAPI required.

## 0.1.0-b20 (2025-03-07)

### Added

- `tiled.server.SimpleTiledServer` can be used for tutorials or development.
  It launches a tiled server on a background thread with basic security.

### Changed

- Added an hdf5plugin import to handle reading lzf-compressed data from Dectris Eiger HDF5 files.
- Removed no-op `?include_data_sources=false` (which is the default) from some
  requests issued by the Python client.
- Added a try-except statement to gracefully skip over broken external links in HDF5 files.

### Maintenance

- Remove a redundant dependency declaration.

## 0.1.0-b19 (2025-02-19)

### Maintenance

- Run authentication tests againts PostgreSQL as well as SQLite.
- Tighten up handling of `time_created` and `time_updated` in authentication
  database.
- New authentication database migration fixes error in migration in previous release.

## 0.1.0-b18 (2025-02-18)

### Added

- Added `SQLAdapter` which can save and interact with table structured data in
  `sqlite` , `postgresql` and `duckdb` databases using `arrow-adbc` API calls.
- Coverage status shows the missing uncovered lines now.
- Added few more tests to `SQLAdapter`.

### Changed

- Removed pydantic-based definitions of structures, which had duplicated
  the dataclass-based defintions in order to work around a pydantic bug
  which has since been resolved. All modules named `tiled.server.pydantic_*`
  have been removed. These were used internally by the server and should
  not affect user code.
- Publish Container image and Helm chart only during a tagged release.
- Stop warning when `data_sources()` are fetched after the item was already
  fetched. (Too noisy.)
- In Tiled's authentication database, when PostgreSQL is used, all datetimes
  are stored explicitly localized to UTC. This requires a database migration
  to update existing rows.

## 0.1.0-b17 (2025-01-29)

### Changed

- Refactor and standardize Adapter API: implement from_uris and from_catalog
  classmethods for instantiation from files and registered Tiled nodes, respectively.
- Refactor CSVAdapter to allow pd.read_csv kwargs
- Removed `tiled.adapters.zarr.read_zarr` utility function.
- Server declares authentication provider modes are `external` or `internal`. The
  latter was renamed from `password`. Client accepts either `internal` or `password`
  for backward-compatibility with older servers.
- Make context switch to HTTPS URI, if available, upon creation

### Added

- Added `.get` methods on TableAdapter and ParquetDatasetAdapter
- Ability to read string-valued columns of data frames as arrays

### Fixed

- Do not attempt to use auth tokens if the server declares no authentication
  providers.
- Prevent "incognito mode" (remember_me=False) from failing after a previous
  login session has since been logged out (no token files)

### Maintenance

- Make dependencies shared by client and server into core dependencies.
- Use schemas for describing server configuration on the client side too.
- Refactored Authentication providers to make use of inheritance, adjusted
  mode in the `AboutAuthenticationProvider` schema to be `internal`|`external`.
  Python clients older than v0.1.0b17 will be sent `password` for back-compat.
- Improved type hinting and efficiency of caching singleton values

## 0.1.0-b16 (2025-01-23)

### Maintenance

- Update GitHub Actions `upload-artifact` and `download-artifact`.

## 0.1.0-b15 (2025-01-23)

### Maintenance

- Adjust for backward-incompatible change in dependency Starlette 0.45.0.

## 0.1.0-b14 (2025-01-21)

### Changed

- Updated OIDC Authenticator configuration to expect `well_known_uri` and
  `audience` and to no longer expect `token_uri`, `authorization_endpoint` and
  `public_keys`, which can be fetched at initialization (server startup) time.
  See examples `example_configs/orcid_auth.yml`,
  `example_configs/google_auth.yml`, and `example_configs/simple_oidc`.

### Maintenance

- Addressed DeprecationWarnings from Python and dependencies
- Update AccessPolicy Docs to match new filter arguments
- Refactored intialization of dask DataFrame to be compatible with dask 2025.1

## v0.1.0-b13 (2025-01-09)

### Added

- `docker-compose.yml` now uses the healthcheck endpoint `/healthz`
- In client, support specifying API key expiration time as string with
  units, like ``"7d"` or `"10m"`.
- Fix bug where access policies were not applied to child nodes during request
- Add metadata-based access control to SimpleAccessPolicy
- Add example test of metadata-based allowed_scopes which requires the path to the target node
- Added Helm chart with deployable default configuration

### Fixed

- Bug in Python client resulted in error when accessing data sources on a
  just-created object.
- Fix bug where access policies were not applied to child nodes during request

### Changed

- The argument `prompt_for_reauthentication` is now ignored and warns.
  Tiled will never prompt for reauthentication after the client is constructed;
  if a session expires or is revoked, it will raise `CannotRefreshAuthentication`.
- The arguments `username` and `password` have been removed from the client
  constructor functions. Tiled will always prompt for these interactively.
  See the Authentication How-to Guide for more information, including on
  how applications built on Tiled can customize this.
- The argument `remember_me` has been added to the client constructor
  functions and to `Context.authenticate` and its alias `Context.login`.
  This can be used to clear and avoid storing any tokens related to
  the session.
- Change access policy API to be async for filters and allowed_scopes
- Pinned zarr to `<3` because Zarr 3 is still working on adding support for
  certain features that we rely on from Zarr 2.


## v0.1.0b12 (2024-12-09)

### Added

- Add HTTP endpoint `PATCH /array/full/{path}` to enable updating and
  optionally _extending_ an existing array.
- Add associated Python client method `ArrayClient.patch`.
- Hook to authentication prompt to make password login available without TTY.

### Fixed

- Fix curl and httpie installation in docker image.
- Minor fix to api key docs to reflect correct CLI usage.
- Fix the construction of urls by passing query parameters as kwargs,
  adapting to a behavior change in httpx v0.28.0.

### Changed

- Switch from appdirs to platformdirs.

## v0.1.0b11 (2024-11-14)

### Added

- Add adapters for reading back assets with the image/jpeg and
  multipart/related;type=image/jpeg mimetypes.
- Automatic reshaping of tiff data by the adapter to account for
  extra/missing singleton dimension
- Add a check for the `openpyxcl` module when importing excel serializer.

### Changed

- Drop support for Python 3.8, which is reached end of life
  upstream on 7 October 2024.
- Do not require SQL database URIs to specify a "driver" (Python
  library to be used for connecting).

### Fixed

- A regression in the container broke support for `tiled register ...` and
  `tiled serve directory ...`. When these became client-side operations, the
  container needed to add the client-side dependencies to support them.

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
