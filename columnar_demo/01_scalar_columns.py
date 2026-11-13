"""Example 1: scalar readings become typed SQL columns.

Each ophyd Signal is a data_key. TiledWriter collects the Events of a stream
into one "internal" table with one row per Event and these columns:

    seq_num, time        -- from the Event document
    <data_key>           -- one per Signal
    ts_<data_key>        -- the timestamp of each reading

The JSON dtype in the descriptor decides the column type:

    Python value  descriptor dtype   Arrow type   PostgreSQL type
    ------------  ----------------   ----------   ----------------
    float         number             double       DOUBLE PRECISION
    int           integer            int64        BIGINT
    bool          boolean            bool         BOOLEAN
    str           string             string       TEXT
"""

import random

import pandas
from common import describe_stream, get_client, make_run_engine, stepped_run
from ophyd import Signal

pandas.set_option("display.width", 200)
pandas.set_option("display.max_columns", 20)

client = get_client()
RE = make_run_engine(client)

temperature = Signal(name="temperature", value=0.0)
frame_number = Signal(name="frame_number", value=0)
shutter_open = Signal(name="shutter_open", value=False)
sample_name = Signal(name="sample_name", value="")

(uid,) = RE(
    stepped_run(
        {
            temperature: lambda i: 295.0 + random.gauss(0, 0.5),
            frame_number: lambda i: i * 10,
            shutter_open: lambda i: i % 2 == 0,
            sample_name: lambda i: f"sample_{i // 3}",
        },
        num=9,
        md={"example": "01_scalar_columns"},
    )
)

stream = describe_stream(client, uid)
table = stream.base["internal"]

print("\nWhole table as a pandas DataFrame:")
print(table.read())

print("\nOnly some columns (only these are fetched from Postgres):")
print(table.read(["temperature", "shutter_open"]))

print("\nA single column, as an array:")
print(table["frame_number"].read())

# A "composite" stream also lets you address columns directly:
print("\nVia the composite stream node, stream['sample_name']:")
print(client[uid]["primary"]["sample_name"].read())
