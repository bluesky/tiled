"""Shared helpers for the columnar database demo scripts.

Environment variables:
    TILED_URI      (default http://localhost:8000)
    TILED_API_KEY  (default "secret", matching compose.yml)
"""

import os

import dask
import numpy as np
from bluesky import RunEngine
from bluesky import plan_stubs as bps
from bluesky import preprocessors as bpp
from bluesky_tiled_plugins import TiledWriter
from ophyd import Signal

from tiled.client import from_uri

# WORKAROUND: DataFrameClient.read() builds a dask dataframe, and dask's
# default "convert-string" option turns object-dtype columns -- which is how
# pandas holds list/ARRAY columns -- into their *string repr*. Disable it so
# ARRAY columns come back as numpy arrays. (read_partition is unaffected.)
dask.config.set({"dataframe.convert-string": False})

TILED_URI = os.environ.get("TILED_URI", "http://localhost:8000")
TILED_API_KEY = os.environ.get("TILED_API_KEY", "secret")


def get_client():
    """Connect to the Tiled server started by compose.yml."""
    return from_uri(TILED_URI, api_key=TILED_API_KEY)


def make_run_engine(client, **tiled_writer_kwargs):
    """A RunEngine that sends every document to Tiled via TiledWriter.

    Extra kwargs go to TiledWriter, e.g. ``max_array_size`` or ``batch_size``.
    """
    RE = RunEngine({})
    RE.subscribe(TiledWriter(client, **tiled_writer_kwargs))
    return RE


def stepped_run(values, num, md=None):
    """A plan that produces ``num`` Events.

    ``values`` maps each ophyd Signal to a function ``i -> value``. At each
    step every signal is set to its new value and then read, so each Signal
    becomes one data_key (i.e. one column) in the "primary" stream.
    """
    signals = list(values)

    @bpp.run_decorator(md=md or {})
    def inner():
        for i in range(num):
            for sig, fn in values.items():
                yield from bps.abs_set(sig, fn(i), wait=True)
            yield from bps.trigger_and_read(signals)

    return (yield from inner())


class RaggedSignal(Signal):
    """A Signal whose array length may change from one reading to the next.

    Declaring ``shape=[None]`` in the descriptor tells TiledWriter the data
    is ragged (variable length).
    """

    def describe(self):
        desc = super().describe()
        desc[self.name]["shape"] = [None]
        desc[self.name]["dtype"] = "array"
        desc[self.name]["dtype_numpy"] = np.dtype(self.get().dtype).str
        return desc


def describe_stream(client, uid, stream="primary"):
    """Print how each node in a stream is stored, and return the stream node."""
    node = client.include_data_sources()[uid][stream]
    print(f"\nRun {uid[:8]}  stream '{stream}'  (spec: {[s.name for s in node.specs]})")
    # A "composite" node presents table columns and arrays side by side as if
    # they were siblings; `.base` shows the actual child nodes that are stored.
    base = getattr(node, "base", node)
    for key in base:
        child = base[key]
        ds = child.data_sources()[0]
        print(
            f"\n  {key!r}: structure_family={child.structure_family}, "
            f"mimetype={ds.mimetype}"
        )
        if child.structure_family == "table":
            schema = child.structure().arrow_schema_decoded
            print(
                f"    SQL table : {ds.parameters['table_name']}  "
                f"(_dataset_id={ds.parameters['dataset_id']})"
            )
            print("    columns   :")
            for field in schema:
                print(f"      {field.name:<20} {field.type}")
        else:
            print(f"    shape={child.shape} dtype={child.dtype}")
    return node
