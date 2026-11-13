"""Example 4: many Tiled tables share one SQL table.

Tiled does not create one SQL table per Tiled table node. The SQL table name is
derived from a hash of the Arrow schema (column names + types):

    table_name = "table_" + md5(arrow_schema).hexdigest()

so every run whose stream has the *same* columns appends rows into the *same*
SQL table, and each run's rows are tagged with their own `_dataset_id`
(`_partition_id` marks the partition within that node). Reading
client[uid]["primary"]["internal"] becomes, roughly,

    SELECT <columns> FROM table_<hash> WHERE _dataset_id = <n> AND _partition_id = 0

A run with a different set of data_keys (or different dtypes) gets a new
table. You can see this from Postgres with ./inspect_sql.sh.
"""

from common import get_client, make_run_engine, stepped_run
from ophyd import Signal

client = get_client().include_data_sources()
RE = make_run_engine(client)

x = Signal(name="x", value=0.0)
y = Signal(name="y", value=0.0)
z = Signal(name="z", value=0)


def internal_table_info(uid):
    ds = client[uid]["primary"].base["internal"].data_sources()[0]
    return ds.parameters["table_name"], ds.parameters["dataset_id"]


rows = []
for n in range(3):
    (uid,) = RE(
        stepped_run(
            {x: lambda i: i * 1.0, y: lambda i, n=n: i * 10.0 + n},
            num=4,
            md={"example": "04_shared_sql_tables", "scan": n},
        ),
    )
    rows.append(("x, y", uid, *internal_table_info(uid)))

# One extra column (z) changes the schema hash -> a different SQL table
(uid,) = RE(
    stepped_run(
        {x: lambda i: i * 1.0, y: lambda i: 0.0, z: lambda i: i},
        num=4,
        md={"example": "04_shared_sql_tables", "scan": "with z"},
    ),
)
rows.append(("x, y, z", uid, *internal_table_info(uid)))

print(f"\n{'data_keys':<10} {'run uid':<10} {'SQL table':<40} _dataset_id")
for keys, uid, table_name, dataset_id in rows:
    print(f"{keys:<10} {uid[:8]:<10} {table_name:<40} {dataset_id}")

first_table = rows[0][2]
print(f"""
The first three runs share {first_table}.
Look at the raw rows in Postgres with:

    ./inspect_sql.sh {first_table}
""")
