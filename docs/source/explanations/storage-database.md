# Storage Database

Tiled employs three SQL databases.

1. A "catalog" database of metadata, structure descriptions, and pointers to
   data (e.g. filepaths);
2. A "storage" database for storing tabular data;
3. An authentication database, supporting Tiled's authentication system.

**This document addresses (2): The "storage" database.**

## Development Status

The "storage" database is the least mature of the three databases. We
anticipate that our thinking is likely to evolve in response to use,
performance testing, and feedback from collaborators.

## Why a storage database at all?

Tiled can store tabular data in Parquet, CSV, or Arrow IPC file formats. These
are suited for batch writing, such as uploading the result of some data
analysis. They are not well suited to _streaming_ data, where writers are
appending rows while readers are reading.

When Tiled is deployed in a horizontally scaled fashion, distributed across
more than one node, any file-based storage needs to be backed by a networked
file system, such as GPFS or NFS. Concurrent writing and reading is at worst
unsafe (NFS v3) and at best slow, due to the challenges of file locking
(`fsync`) on distributed file systems. (Blob storage addresses some of these
problems, but at the cost of _eventual_ consistency and append-only writes.)

This is a problem that databases solve: enabling atomic, concurrent writes and
reads. The storage database was introduced into Tiled to enable safe streaming
of tabular data with concurrent readers. This includes data from Bluesky Events
in particular, which will have one writer and potentially many concurrent readers.
But we also anticipate use cases for streaming _processed_ data, which may have
multiple concurrent writers as well. Directly backing this with files would be
intractable: we need a database.

## Which database management system (DBMS) should I use?

The "catalog" and authentication database are straightforward traditional OLTP
(OnLine Transaction Processing) databases. Most queries are looking for a
needle in a hackstack: querying for a single row or a page of ~100 rows.

The use case for Tiled's "storage" database may include some OLAP (OnLine
Analytical Processing) workloads, fetching or performing reduction queries
on a large fraction of the rows in a table. The data tends to be numerical,
and expressing correct numerical precision (e.g. uint8 versus "integer")
may be important for fidelity, speed, and storage efficiency.

The choice of "OLAP" database is a large and growing space, currently receiving
a lot of attention. Well-known projects include [DuckDB](https://duckdb.org/)
and [ClickHouse](https://clickhouse.com/); there are many, many others in the
category.

To start, Tiled supports these three backends:

- PostgreSQL (recommended for production)
- DuckDB (default for single-process dev)
- SQLite

Of these, only DuckDB is OLAP, and notably it can only be connected to a single
process at a time, so it is not compatible with production, horizontally-scaled
Tiled deployments.

We recommend PostgreSQL for production because the _stewardship_ of the data is
the paramount concern, and PostgreSQL is extremely well trusted not to lose or
corrupt data. We received a strong recommendation from professional SQL developers
familiar with our use case to start with PostgreSQL. We are likely to stay there
for the next 1–2 years at least.

We are watching other projects, including the DuckDB integration with PostgreSQL
which would leverage DuckDB's richer numerical type system within PostgreSQL.

## What schema?

When Bluesky emits an Event Descriptor, it defines a schema for the Events (rows)
to follow. These schemas are often repeated, but can be unique, defined by
some arbitrary collection of measured signals.

Tiled receives this as an Arrow schema (names and data types) followed by
batches of rows. It supplements these rows with a `dataset_id` and a
`partition_id` and inserts them into a table. By default, it uses a table
with a name derived from the schema, `table_{arrow_schema_hash}`, and thus
`dataset_id` is necessary to identify the rows comprising logically related
data, e.g. coming from the same Bluesky run. The client has the option to
specify a meaningful, human-readable name if preferred. If a matching table
does not already exist, Tiled creates it. (This too, could be customized.
Perhaps some instruments would want to only use tables that are pre-declared
and purposefully named.)

table_{schema_hash}
dataset_id | partition_id | x | y | temp
-- | -- | -- | -- | --
42 | 1  | .  | .  | .
42 | 1  | .  | .  | .
43 | 1  | .  | .  | .
43 | 1  | .  | .  | .
44 | 1  | .  | .  | .
44 | 1  | .  | .  | .


table_{schema_hash}
dataset_id | partition_id | y | temp | z
-- | -- | -- | -- | --
45 | 1  | .  | .  | .
45 | 1  | .  | .  | .
45 | 1  | .  | .  | .
45 | 2  | .  | .  | .
45 | 2  | .  | .  | .

This means, the number of tables scales with the number of unique schemas---
roughly speaking, the number of distinct types of experiments.

Currently, Tiled merely fetches data, filtered by `dataset_id` and optionally
by `partition_id` (currently, we use a single partition for all datasets by
default). We foresee exposing the capability to query relations across columns
(`SELECT ... WHERE ...`) or to do _limited_ server-side
reductions (`SELECT MAX(...) WHERE ...`). Filtering data based on value has
been a frequent request from users.

## Why "partition"?

A partition is a "chunk" of a single dataset. Typically, many datasets may only
have a single partition.

For file-based storage, data is commonly partitioned into files, like
`data_01.csv`, `data_02.csv`, .... Tiled exposes APIs to fetch data by
partition, to facilitate partial and parallel access.

The `partition_id` is designed to facilitate the same for data stored in
a database.

Potentially, row range slicing could be supported as well. But this is subtle.
Unlike array data, tabular data does not necessary come with a natural sorting
or fixed position for each row. On-disk formats like Parquet _do_ have a
natural order -— the order the rows are stored in -— but data stored in SQL
does not necessarily have a default or even a unique order. For _Bluesky_ data,
timestamps would be the obvious choice, but again we have in mind broader use
cases, including analysis results.

Thus, partitioning gives a way to unambiguously chunk a large dataset.

Notice that PostgreSQL table partitioning is a totally distinct concept. That
might also be useful, but has no relationship to Tiled's application-level
`partition_id`.

## Arrow Database Connectivity (ADBC)

Tiled describes tabular data, whether stored in files or in databases, using
[Apache Arrow](https://arrow.apache.org/) semantics. Apache Arrow is the
industry standard for describing columnar data, and it is portable across
programming languages.

Arrow Database Connectivity provides a way to insert columnar data into SQL
databases without first transposing it into individual rows. The performance
of inserting a table of memory-contigous columnar buffers is far superior
to the performance of first "exploding" that table into individual records
(per row) and inserting those (even in a batch).

More performance testing is needed to measure the impact on our use cases, but
ADBC seems to be the right technology for using Arrow tables with SQL storage.
