<!-- Add the recent changes in the code under the relevant category.
Write the date in place of the "Unreleased" in the case a new version is released. -->

# Changelog

## Unreleased

### Added

* Support specifying the format that uploaded data will be stored in.
* Support storing uploaded tabular data in CSV format.
* Added a new HTTP endpoint, `PATCH /api/v1/table/partition/{path}`
  supporting appending rows to a tabular dataset.
* Added a new method `DataFrameClient.append_partition`.

### Removed

### Changed

### Fixed
