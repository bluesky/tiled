"""Example 2: array readings -- some become ARRAY columns, some do not.

TiledWriter decides per data_key where array data lives:

    shape                    size vs max_array_size   stored as
    -----------------------  -----------------------  ----------------------------
    fixed, numeric, 1D       <= max_array_size (16)   ARRAY column in the SQL table
                                                      (e.g. DOUBLE PRECISION[])
    fixed, numeric           >  max_array_size         separate zarr array node
    fixed, strings           <= max_array_size         TEXT[] column, unless the descriptor
                                                      declares a string dtype_numpy (e.g.
                                                      "<U8"), in which case it goes to zarr
    contains None (ragged)   any                       separate ragged node (example 3)

Setting TiledWriter(max_array_size=-1) puts every fixed-shape numeric array
into the table, and max_array_size=0 puts none of them there.
"""

import numpy as np
import pandas
from common import describe_stream, get_client, make_run_engine, stepped_run
from ophyd import Signal

pandas.set_option("display.width", 200)
pandas.set_option("display.max_columns", 20)
pandas.set_option("display.max_colwidth", 40)

client = get_client()
rng = np.random.default_rng(0)

roi_sums = Signal(name="roi_sums", value=np.zeros(4))  # 4 floats   -> column
motor_xyz = Signal(name="motor_xyz", value=np.zeros(3, dtype="int32"))  # -> column
spectrum = Signal(name="spectrum", value=np.zeros(256))  # 256 > 16 -> zarr
# A plain ophyd Signal does not declare dtype_numpy, so this becomes TEXT[]
channel_names = Signal(name="channel_names", value=np.array(["a", "b"]))
energy = Signal(name="energy", value=0.0)  # a scalar column, for contrast

signals = {
    roi_sums: lambda i: rng.random(4) * 100,
    motor_xyz: lambda i: np.array([i, 2 * i, 3 * i], dtype="int32"),
    spectrum: lambda i: rng.poisson(10, 256).astype("float64"),
    channel_names: lambda i: np.array([f"ch{i}a", f"ch{i}b"]),
    energy: lambda i: 8000.0 + 10 * i,
}

print("=" * 70)
print("Default TiledWriter (max_array_size=16)")
print("=" * 70)
RE = make_run_engine(client)
(uid,) = RE(stepped_run(signals, num=5, md={"example": "02_array_columns"}))
stream = describe_stream(client, uid)
print("\nThe internal table (roi_sums, motor_xyz, channel_names are ARRAY columns):")
df = stream.base["internal"].read(
    ["seq_num", "energy", "roi_sums", "motor_xyz", "channel_names"]
)
print(df)
print("\nEach cell of an ARRAY column is a numpy array:", repr(df["motor_xyz"][4]))
print("\nspectrum lives in its own array node; shape (num_events, 256):")
print(stream["spectrum"].shape)

print()
print("=" * 70)
print("TiledWriter(max_array_size=-1): every numeric array goes into the table")
print("=" * 70)
RE = make_run_engine(client, max_array_size=-1)
(uid,) = RE(
    stepped_run(signals, num=5, md={"example": "02_array_columns_all_in_table"})
)
stream = describe_stream(client, uid)
spectrum = stream.base["internal"].read(["spectrum"])["spectrum"]
print(
    f"\nspectrum is now a DOUBLE PRECISION[] column: {len(spectrum)} rows, "
    f"each an array of {len(spectrum[0])} values"
)
