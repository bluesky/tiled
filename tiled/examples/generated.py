import asyncio
import string
import sys
from datetime import timedelta

import numpy
import pandas
import sparse
import xarray

from tiled.adapters.array import ArrayAdapter
from tiled.adapters.dataframe import DataFrameAdapter
from tiled.adapters.mapping import MapAdapter
from tiled.adapters.sparse import COOAdapter
from tiled.adapters.xarray import DatasetAdapter

print("Generating large example data...", file=sys.stderr)
data = {
    "big_image": numpy.random.random((10_000, 10_000)),
    "small_image": numpy.random.random((300, 300)),
    "medium_image": numpy.random.random((1000, 1000)),
    "tiny_image": numpy.random.random((50, 50)),
    "tiny_cube": numpy.random.random((50, 50, 50)),
    "tiny_hypercube": numpy.random.random((50, 50, 50, 50, 50)),
    "high_entropy": numpy.random.random((100, 100)),
    "low_entropy": numpy.ones((100, 100)),
    "short_column": numpy.random.random(100),
    "tiny_column": numpy.random.random(10),
    "long_column": numpy.random.random(100_000),
}
temp = 15 + 8 * numpy.random.randn(2, 2, 3)
precip = 10 * numpy.random.rand(2, 2, 3)
lon = [[-99.83, -99.32], [-99.79, -99.23]]
lat = [[42.25, 42.21], [42.63, 42.59]]
sparse_arr = numpy.random.random((100, 100))
sparse_arr[sparse_arr < 0.9] = 0  # fill most of the array with zeros

print("Done generating example data.", file=sys.stderr)

mapping = {
    "big_image": ArrayAdapter.from_array(data["big_image"]),
    "small_image": ArrayAdapter.from_array(data["small_image"]),
    "medium_image": ArrayAdapter.from_array(data["medium_image"]),
    "sparse_image": COOAdapter.from_coo(sparse.COO(sparse_arr)),
    "tiny_image": ArrayAdapter.from_array(data["tiny_image"]),
    "tiny_cube": ArrayAdapter.from_array(data["tiny_cube"]),
    "tiny_hypercube": ArrayAdapter.from_array(data["tiny_hypercube"]),
    "short_table": DataFrameAdapter.from_pandas(
        pandas.DataFrame(
            {
                "A": data["short_column"],
                "B": 2 * data["short_column"],
                "C": 3 * data["short_column"],
            },
            index=pandas.Index(numpy.arange(len(data["short_column"])), name="index"),
        ),
        npartitions=1,
        metadata={"animal": "dog", "color": "red"},
    ),
    "long_table": DataFrameAdapter.from_pandas(
        pandas.DataFrame(
            {
                "A": data["long_column"],
                "B": 2 * data["long_column"],
                "C": 3 * data["long_column"],
            },
            index=pandas.Index(numpy.arange(len(data["long_column"])), name="index"),
        ),
        npartitions=5,
        metadata={"animal": "dog", "color": "green"},
    ),
    "wide_table": DataFrameAdapter.from_pandas(
        pandas.DataFrame(
            {
                letter: i * data["tiny_column"]
                for i, letter in enumerate(string.ascii_uppercase, start=1)
            },
            index=pandas.Index(numpy.arange(len(data["tiny_column"])), name="index"),
        ),
        npartitions=1,
        metadata={"animal": "dog", "color": "red"},
    ),
    "structured_data": MapAdapter(
        {
            "pets": ArrayAdapter.from_array(
                numpy.array(
                    [("Rex", 9, 81.0), ("Fido", 3, 27.0)],
                    dtype=[("name", "U10"), ("age", "i4"), ("weight", "f4")],
                )
            ),
            "xarray_dataset": DatasetAdapter.from_dataset(
                xarray.Dataset(
                    {
                        "temperature": (["x", "y", "time"], temp),
                        "precipitation": (["x", "y", "time"], precip),
                    },
                    coords={
                        "lon": (["x", "y"], lon),
                        "lat": (["x", "y"], lat),
                        "time": pandas.date_range("2014-09-06", periods=3),
                    },
                ),
            ),
        },
        metadata={"animal": "cat", "color": "green"},
    ),
    "flat_array": ArrayAdapter.from_array(numpy.random.random(100)),
    "low_entropy": ArrayAdapter.from_array(data["low_entropy"]),
    "high_entropy": ArrayAdapter.from_array(data["high_entropy"]),
    # Below, an asynchronous task modifies this value over time.
    "dynamic": ArrayAdapter.from_array(numpy.zeros((3, 3))),
}
# The entries aren't actually dynamic, but set entries_stale_after
# to demonstrate cache expiry.
tree = MapAdapter(mapping, entries_stale_after=timedelta(seconds=10))


async def increment_dynamic():
    """
    Change the value of the 'dynamic' node every 3 seconds.
    """
    fill_value = 0
    while True:
        fill_value += 1
        mapping["dynamic"] = ArrayAdapter.from_array(fill_value * numpy.ones((3, 3)))
        await asyncio.sleep(3)


# The server will run this on its event loop. We cannot start it *now* because
# there is not yet a running event loop.
tree.background_tasks.append(increment_dynamic)
