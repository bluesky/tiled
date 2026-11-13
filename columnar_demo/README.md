# Tiled columnar database demo

This demo starts Tiled with PostgreSQL. Bluesky runs are written to it with
[`TiledWriter`](https://blueskyproject.io/bluesky/main/tiled-writer.html),
so you can see how each kind of reading ends up in Tiled's SQL ("columnar")
storage.

## Start the stack

```bash
cd columnar_demo
docker compose up -d          # or: podman compose up -d
```

* Tiled: <http://localhost:8000> (API key `secret`)
* PostgreSQL: `localhost:5432` (user/password `tiled`/`tiled`), with two databases:
  * `tiled_catalog`: the node tree, metadata, and data source pointers
  * `tiled_storage`: the tabular data itself (the columnar database)

Arrays that don't belong in a table (large waveforms, images) are written as
zarr to the `tiled_data` volume.

You can override `TILED_API_KEY`, `POSTGRES_PASSWORD` and `TILED_VERSION` in
the environment or in a `.env` file. To run this checkout's code instead of a
released image, swap `image:` for the commented `build:` block in `compose.yml`.

## Run the examples

The client side is a [uv](https://docs.astral.sh/uv/) project (`pyproject.toml`
and `uv.lock`, with Python pinned to 3.12 in `.python-version`). `uv run`
creates `.venv` and installs the locked dependencies the first time:

```bash
uv run 01_scalar_columns.py
```

## Examples

| Script | Shows |
|---|---|
| `01_scalar_columns.py` | float, int, bool and str readings become `DOUBLE PRECISION`, `BIGINT`, `BOOLEAN` and `TEXT` columns. Also shows reading whole tables, selected columns and single columns. |
| `02_array_columns.py` | Small fixed-shape arrays (≤ `max_array_size`, default 16) become `ARRAY` columns (`DOUBLE PRECISION[]`, `INTEGER[]`, `TEXT[]`). Larger arrays get their own zarr node. Also shows `max_array_size=-1`. |
| `03_ragged_columns.py` | Variable-length arrays (`shape=[None]`) go to a separate ragged node, stored in its own SQL table of offsets and data buffers. |
| `04_shared_sql_tables.py` | Runs with identical schemas share one SQL table, with rows told apart by `_dataset_id`. A different schema gets a new table. |
| `05_all_column_types.py` | Every Arrow → PostgreSQL type mapping, using the Tiled client directly (TiledWriter only produces float64/int64/bool/string), plus partitions and appends. |

```bash
for f in 0*.py; do uv run "$f"; done   # run them all
```

## Look at the raw SQL

```bash
./inspect_sql.sh                 # every table, its column types and row count
./inspect_sql.sh table_<hash>    # one table's definition and first rows
COMPOSE="podman compose" ./inspect_sql.sh   # with podman
```

## How TiledWriter lays out a run

```
<run uid>                    Container, spec BlueskyRun_v3 (start/stop docs in metadata)
└── primary                  Container, specs BlueskyEventStream + composite
    ├── internal             Table   -> one row per Event, in tiled_storage
    │     seq_num, time, <scalar keys>, <small array keys>, ts_<every key>
    ├── spectrum             Array   -> zarr        (array larger than max_array_size)
    └── peak_positions       Ragged  -> SQL buffers (shape contains None)
```

Because the stream is a `composite` node, `client[uid]["primary"]["temperature"]`
reads a column as if it were a sibling of the arrays. Use
`client[uid]["primary"].base["internal"]` to get the table itself.

## Known issues found while writing this

* `DataFrameClient.read()` returns `ARRAY` columns as **strings** (e.g.
  `"[1. 2. 3.]"`). The client builds a dask dataframe, and dask's default
  `dataframe.convert-string` setting turns object-dtype columns into strings.
  `read_partition()` is not affected. `common.py` works around this with
  `dask.config.set({"dataframe.convert-string": False})`.
* Creating a table with a column type Postgres storage doesn't support
  (`uint64`, timestamps, structs, ...) gives HTTP 500 instead of a 4xx error.

## Reset

```bash
docker compose down -v   # removes the postgres and zarr volumes
```
