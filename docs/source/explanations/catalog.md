# Catalog Database

The Catalog database is a SQL database of information describing data: its
name, metadata, structure, format, and location.

## Overview

```{mermaid}
erDiagram
    nodes ||--o{  data_sources : has
    data_sources ||--o{  data_source_asset_association : has
    data_source_asset_association }|--|{  assets : has
    data_sources }|--||   structure : has
    nodes ||--o{  revisions : has
    alembic_version

```

- `nodes` - metadata and logical location of this dataset in Tiled's tree
- `data_sources` - format and parameters for opening dataset
- `structures` - description of dataset structure (e.g. shape, chunks, data type, column names, ...)
- `assets` - location (URI) of data
- `data_source_asset_assocation` - many-to-many relation between `data_sources` and `assets`
- `revisions` - snapshots of revision history of metadata
- `alembic_version` - version of database schema, to verify compatibility with version of Tiled

## Nodes

The `nodes` table is the _logical_ view of the data, the way that Tiled
presents the data to clients. Each row represents one node in the logical
"tree" of data represented by Tiled.

- `metadata` --- user-controlled JSON object, with arbitrary metadata
- `key` --- the name of the Node; together with ancestors specify the unique path of the data
- `structre_family` --- enum of structure types (`"container"`, `"array"`, `"table"`, ...)
- `specs` --- user-controlled JSON list of specs, such as `[{"name": "XDI", "version": "1"}]`
- `id` --- an internal integer primary key, not exposed by the API
- `parent` --- the `id` of the node's parent
- `time_created` and `time_updated` --- for forensics, not exposed by the API

The `time_created` and `time_updated` columns, which appear in this table and
others below, contain timestamps related to the corresponding database row
(Node, Data Source, Asset), not the underlying data files. They should not
carry a scientific meaning; they are only used for book-keeping, forensics,
and debugging.

## Data Source

Each Data Source is associated with one Node. Together, `data_sources`, `structures`,
and `assets`, describes the format,  structure, and location of the data.

- `mimetype` --- MIME type string describing the format, such as `"text/csv"`
  (This is used by Tiled to identify a suitable Adapter to read this data.)
- `parameters` --- JSON object with additional parameters that will be passed
  to the Adapter
- `management` --- enum indicating whether the data is registered `"external"` data
  or `"writable"` data managed by Tiled
- `structure_family` --- enum of structure types (`"container"`, `"array"`, `"table"`, ...)
- `structure_id` --- a foreign key to the `structures` table
- `node_id` --- foreign key to `nodes`
- `id` --- integer primary key
- `time_created` and `time_updated` --- for forensics, not exposed by the API

## Structure

Each Data Source references exactly one Structure.

- `structure` --- JSON object describing the structure
- `id` --- MD5 hash of the [RFC 8785][] canonical JSON of the structure

## Asset

- `data_uri` --- location of data, given as `file://localhost/PATH`
  (It is planned to extend to schemes other than `file`, such as `s3`, in the
  future.)
- `is_directory` --- boolean: `true` when the Asset being tracked is a
  directory. This is used for data formats in which the directory structure is
  an internal detail managed by the I/O library, such as Zarr and TileDB.
  Otherwise this is `false`, and Tiled tracks each file as an individual Asset,
  such as each TIFF file in a TIFF sequence, or each HDF5 file in a virtual
  HDF5 dataset).
- `hash_type` and `hash_content` --- not yet implemented (i.e. always NULL) but
  intended for content verification
- `size` --- not yet implemented (i.e. always NULL) but intended to support
  fast queries for data size estimation
- `id` --- integer primary key
- `time_created` and `time_updated` --- for forensics, not exposed by the API

## Data Source Asset Relation

Assets and Data Sources have a many-to-many relation. The
`data_source_asset_assocation` table is best described by the example below.

- `data_source_id`, `asset_id` --- foreign keys
- `parameter` --- the name of the Tiled Adapter's parameter that this Asset
  should be passed to, e.g. `"data_uri"` or `"data_uris"`. These can be any
  string because some Adapters handle a heterogeneous group of Assets, like
  a combination of an image file and a separate text metadata file, and
  load them as a unit. The parameter is used to differentiate the various
  Assets for the Adapter.
- `num` --- the position of this item in a list

If `parameter` is NULL, the Asset is a supporting file, not passed directly to
the Adapter.

If `num` is NULL, the Adapter will be passed a scalar value. If `num` is an
integer, the Adapter will be passed a list sorted by `num`.

Database triggers are used to ensure self-consistency.

### Single HDF5 file

This is a simple example: one Data Source and one associated Asset.

```sql
select id, mimetype, parameters from data_sources;
```

id | mimetype | parameters |
-- | -- | --
1 | "application/x-hdf5" | {"smwr": true}


```sql
select data_uri, is_diretory from assets
```

id | data_uri | is_directory
-- | -- | --
1 | "file://localhost/path/to/data.h5" | false

The HDF5 Adapter takes one HDF5 file passed to the argument
named `data_uri`, so the Asset is given parameter `"data_uri"`
and num `NULL`.

```sql
select * from data_source_asset_assocation
```

data_source_id | asset_id | parameter | num
-- | -- | -- | --
1 | 1 | "data_uri" | NULL

### Single Zarr directory

This is similar. A single Zarr dataset is backed by a directory, not a
file. The internal structure of the directory is managed by Zarr, not by the
user, so Tiled can simply track the whole directory as a unit, not each
individual file.

```sql
select id, mimetype, parameters from data_sources;
```

id | mimetype | parameters |
-- | -- | --
1 | "application/x-zarr" | {}


```sql
select data_uri, is_diretory from assets
```

id | data_uri | is_directory
-- | -- | --
1 | "file://localhost/path/to/data.zarr" | true

(Notice `is_directory` is `true`.)

```sql
select * from data_source_asset_assocation
```

data_source_id | asset_id | parameter | num
-- | -- | -- | --
1 | 1 | "data_uri" | NULL

### Single TIFF Image

This is another simple example, very much like the HDF5 example.

```sql
select id, mimetype, parameters from data_sources;
```

id | mimetype | parameters |
-- | -- | --
1 | "image/tiff" | {} | NULL


```sql
select data_uri, is_diretory from assets
```

id | data_uri | is_directory
-- | -- | --
1 | "file://localhost/path/to/image.tiff" | false

```sql
select * from data_source_asset_assocation
```

data_source_id | asset_id | parameter | num
-- | -- | -- | --
1 | 1 | "data_uri" | NULL

### TIFF sequence

Now we have a sequence of separate TIFF files (`image00001.tiff`,
`image00002.tiff`, ...) that we want to treat as a single Data Source.

```sql
select id, mimetype, parameters from data_sources;
```

id | mimetype | parameters |
-- | -- | --
1 | "multipart/related;type=image/tiff" | {}

The MIME type `multipart/related;type=image/tiff` is registered to an Adapter
that expects a _sequence_ of TIFF files, e.g. `TiffSequenceAdapter`.

```sql
select data_uri, is_diretory from assets
```

id | data_uri | is_directory
-- | -- | --
1 | "file://localhost/path/to/image00001.tiff" | false
2 | "file://localhost/path/to/image00002.tiff" | false
3 | "file://localhost/path/to/image00003.tiff" | false

```sql
select * from data_source_asset_assocation
```

data_source_id | asset_id | parameter | num
-- | -- | -- | --
1 | 1 | "data_uris" | 0
1 | 2 | "data_uris" | 1
1 | 3 | "data_uris" | 2

### Single CSV file

The CSV Adapter is designed to accept multiple CSV partitions
representing batches (a.k.a. partitions) of rows.

```sql
select id, mimetype, parameters from data_sources;
```

id | mimetype | parameters |
-- | -- | --
1 | "text/csv" | {} | NULL


```sql
select data_uri, is_diretory from assets
```

id | data_uri | is_directory
-- | -- | --
1 | "file://localhost/path/to/table.csv" | false

The CSV Adapter takes one or more CSV passed as a list to the
argument named `data_uris`, so the Asset is given parameter
`data_uris` and num `0`.

```sql
select * from data_source_asset_assocation
```

data_source_id | asset_id | parameter | num
-- | -- | -- | --
1 | 1 | "data_uris" | 0

### HDF5 file with virtual datasets

Here is an example where we set parameter to NULL.

```sql
select id, mimetype, parameters from data_sources;
```

id | mimetype | parameters |
-- | -- | --
1 | "application/x-hdf5" | {}


```sql
select data_uri, is_diretory from assets
```

id | data_uri | is_directory
-- | -- | --
1 | "file://localhost/path/to/master.h5" | false
2 | "file://localhost/path/to/data00001.h5" | false
3 | "file://localhost/path/to/data00002.h5" | false
4 | "file://localhost/path/to/data00003.h5" | false

The CSV Adapter takes one or more CSV passed as a list to the
argument named `data_uris`, so the Asset is given parameter
`data_uris` and num `0`.

```sql
select * from data_source_asset_assocation
```

data_source_id | asset_id | parameter | num
-- | -- | -- | --
1 | 1 | "data_uri" | NULL
1 | 2 | NULL | NULL
1 | 3 | NULL | NULL
1 | 4 | NULL | NULL

## Revisions

The `revisions` table stores snapshots of Node `metadata` and `specs`. When an
update is made, the row in the `nodes` table is updated and a _copy_ with the
original content is inserted in the `revisions` table.

- `node_id` --- foreign key to the node
- `revision_number` --- integer counting revisions of this node from 1
- `metadata` --- snapshot of node metadata
- `specs` --- snapshot of node specs
- `id` --- an internal integer primary key, not exposed by the API
- `time_created` and `time_updated` --- for forensics, not exposed by the API

## Alembic Version

The `alembic_version` table is managed by [Alembic][], a SQL migration tool, to
stamp the current version of the database. The Tiled server checks this at
startup to ensure that the version of Tiled being used is compatible with the
version of the database.

[RFC 8785]: https://www.rfc-editor.org/rfc/rfc8785
[Alembic]: https://alembic.sqlalchemy.org/en/latest/
