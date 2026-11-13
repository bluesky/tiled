"""Example 3: variable-length (ragged) arrays are kept out of the SQL table.

When a descriptor declares a shape containing None (e.g. a peak list whose
length changes every event), TiledWriter writes the data_key to a separate
"ragged" node instead of a column of the stream's table. The table keeps only
the timestamp column `ts_<key>` for it.

The ragged node is still stored in PostgreSQL (mimetype
application/x-ragged+sql), but in its own SQL table holding the awkward-array
buffers rather than one value per event:

    _dataset_id | _partition_id | node0-offsets BIGINT[] | node1-data DOUBLE PRECISION[] | chunk_index

`node1-data` is all the values concatenated, and `node0-offsets` marks where
each event's row starts and ends ([0, 3, 4, 9, 11, 15] for the lengths below).
"""

import numpy as np
from common import (
    RaggedSignal,
    describe_stream,
    get_client,
    make_run_engine,
    stepped_run,
)
from ophyd import Signal

client = get_client()
RE = make_run_engine(client)
rng = np.random.default_rng(1)

peak_positions = RaggedSignal(name="peak_positions", value=np.zeros(1))
num_peaks = Signal(name="num_peaks", value=0)

lengths = [3, 1, 5, 2, 4]
(uid,) = RE(
    stepped_run(
        {
            peak_positions: lambda i: np.sort(rng.uniform(0, 100, lengths[i])),
            num_peaks: lambda i: lengths[i],
        },
        num=len(lengths),
        md={"example": "03_ragged_columns"},
    )
)

stream = describe_stream(client, uid)
print("\nThe table holds the fixed-width data:")
print(stream.base["internal"].read(["seq_num", "num_peaks", "ts_peak_positions"]))

print("\nThe ragged node holds one variable-length row per event:")
ragged = stream["peak_positions"].read()  # a `ragged.array`
print(f"  type={type(ragged).__name__}, shape={ragged.shape}")
for i, row in enumerate(ragged.tolist()):
    print(f"  event {i + 1}: {[round(x, 1) for x in row]}")
