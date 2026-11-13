"""Example 5: every column type the PostgreSQL backend supports.

TiledWriter only produces float64 / int64 / bool / string columns (plus lists
of them), because that is what Bluesky descriptors express. The Tiled client
can create a table from any Arrow schema directly, which shows the complete
type mapping in tiled/adapters/sql.py (ARROW_TO_PG_TYPES):

    Arrow          PostgreSQL           note
    -------------  -------------------  -----------------------------------
    bool           BOOLEAN
    int8 / int16   SMALLINT             no 1-byte int in PG
    int32          INTEGER
    int64          BIGINT
    uint8          SMALLINT             unsigned -> next signed size up
    uint16         INTEGER
    uint32         BIGINT
    float32        REAL
    float64        DOUBLE PRECISION
    string         TEXT
    large_string   TEXT
    list<T>        <PG type of T>[]     any of the above as T

Anything else (uint64, timestamps, dates, decimals, binary, structs) is
rejected when the table is created. (Currently the server reports this as a
500 Internal Server Error -- "ValueError: Unsupported PyArrow type" in the
server log -- and the client retries a few times before giving up.)
"""

import numpy as np
import pandas
import pyarrow as pa
from common import get_client

pandas.set_option("display.width", 200)
pandas.set_option("display.max_columns", 20)

client = get_client()
demo = client.create_container(metadata={"example": "05_all_column_types"})

schema = pa.schema(
    [
        ("flag", pa.bool_()),
        ("i8", pa.int8()),
        ("i16", pa.int16()),
        ("i32", pa.int32()),
        ("i64", pa.int64()),
        ("u8", pa.uint8()),
        ("u16", pa.uint16()),
        ("u32", pa.uint32()),
        ("f32", pa.float32()),
        ("f64", pa.float64()),
        ("label", pa.string()),
        ("notes", pa.large_string()),
        ("roi", pa.list_(pa.float32())),
        ("pixel_ids", pa.list_(pa.int16())),
        ("tags", pa.list_(pa.string())),
    ]
)


def make_rows(start, n):
    i = np.arange(start, start + n)
    return pa.Table.from_pydict(
        {
            "flag": i % 2 == 0,
            "i8": (i - 3).astype("int8"),
            "i16": (i * 1000).astype("int16"),
            "i32": (i * 100_000).astype("int32"),
            "i64": i * 10_000_000_000,
            "u8": (i * 20).astype("uint8"),
            "u16": (i * 5_000).astype("uint16"),
            "u32": (i * 300_000_000).astype("uint32"),
            "f32": (i / 3).astype("float32"),
            "f64": i / 3,
            "label": [f"row {k}" for k in i],
            "notes": ["x" * int(k) for k in i],
            "roi": [np.full(2, k, dtype="float32") for k in i],
            "pixel_ids": [np.arange(k % 4, dtype="int16") for k in i],
            "tags": [["even" if k % 2 == 0 else "odd", f"t{k}"] for k in i],
        },
        schema=schema,
    )


# npartitions=2: each partition is a separate set of rows in the same SQL
# table, distinguished by the _partition_id column.
table = demo.create_appendable_table(schema, npartitions=2, key="all_types")
table.append_partition(0, make_rows(0, 3))  # rows can be appended...
table.append_partition(0, make_rows(3, 2))  # ...repeatedly
table.append_partition(1, make_rows(10, 2))

print("Arrow schema as stored by Tiled:")
print(table.structure().arrow_schema_decoded)

print("\nPartition 0:")
print(table.read_partition(0))
print("\nPartition 1:")
print(table.read_partition(1))
print("\nWhole table (all partitions):", len(table.read()), "rows")

ds = client.include_data_sources()[demo.item["id"]]["all_types"].data_sources()[0]
print(
    f"\nSQL table: {ds.parameters['table_name']}  -> ./inspect_sql.sh {ds.parameters['table_name']}"
)

print("\nUnsupported types are rejected:")
for name, typ in [
    ("uint64", pa.uint64()),
    ("timestamp", pa.timestamp("us")),
    ("struct", pa.struct([("a", pa.int32())])),
]:
    try:
        demo.create_appendable_table(pa.schema([("col", typ)]), key=f"bad_{name}")
    except Exception as err:
        print(f"  {name:<10} -> rejected ({type(err).__name__}: {str(err)[:40]}...)")
    else:
        print(f"  {name:<10} -> accepted (unexpected)")
