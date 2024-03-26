<!-- Add the recent changes in the code under the relevant category.
Write the date in place of the "Unreleased" in the case a new version is released. -->

# Changelog

## v0.1.0a117

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
