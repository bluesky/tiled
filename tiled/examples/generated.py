import asyncio
import sys

import dask.array
import dask.dataframe
import numpy
import pandas
import xarray

from tiled.readers.array import ArrayAdapter, StructuredArrayTabularAdapter
from tiled.readers.dataframe import DataFrameAdapter
from tiled.readers.xarray import DataArrayAdapter, DatasetAdapter, VariableAdapter
from tiled.trees.in_memory import Tree


print("Generating large example data...", file=sys.stderr)
data = {
    "big_image": numpy.random.random((10_000, 10_000)),
    "small_image": numpy.random.random((100, 100)),
    "medium_image": numpy.random.random((1000, 1000)),
    "tiny_image": numpy.random.random((10, 10)),
    "tiny_cube": numpy.random.random((10, 10, 10)),
    "tiny_hypercube": numpy.random.random((10, 10, 10, 10, 10)),
    "high_entropy": numpy.random.random((100, 100)),
    "low_entropy": numpy.ones((100, 100)),
    "short_column": numpy.random.random(100),
    "long_column": numpy.random.random(100_000),
}
print("Done generating example data.", file=sys.stderr)

mapping = {
    "big_image": ArrayAdapter.from_array(data["big_image"]),
    "small_image": ArrayAdapter.from_array(data["small_image"]),
    "medium_image": ArrayAdapter.from_array(data["medium_image"]),
    "tiny_image": ArrayAdapter.from_array(data["tiny_image"]),
    "tiny_cube": ArrayAdapter.from_array(data["tiny_cube"]),
    "tiny_hypercube": ArrayAdapter.from_array(data["tiny_hypercube"]),
    "low_entropy": ArrayAdapter.from_array(data["low_entropy"]),
    "high_entropy": ArrayAdapter.from_array(data["high_entropy"]),
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
    "labeled_data": Tree(
        {
            "image_with_dims": VariableAdapter(
                xarray.Variable(
                    data=dask.array.from_array(data["medium_image"]),
                    dims=["x", "y"],
                    attrs={"thing": "stuff"},
                ),
            ),
        }
    ),
    "structured_data": Tree(
        {
            "pets": StructuredArrayTabularAdapter.from_array(
                numpy.array(
                    [("Rex", 9, 81.0), ("Fido", 3, 27.0)],
                    dtype=[("name", "U10"), ("age", "i4"), ("weight", "f4")],
                )
            ),
            "image_with_coords": DataArrayAdapter(
                xarray.DataArray(
                    xarray.Variable(
                        data=dask.array.from_array(data["medium_image"]),
                        dims=["x", "y"],
                        attrs={"thing": "stuff"},
                    ),
                    coords={
                        "x": dask.array.arange(len(data["medium_image"])) / 10,
                        "y": dask.array.arange(len(data["medium_image"])) / 50,
                    },
                ),
            ),
            "xarray_dataset": DatasetAdapter(
                xarray.Dataset(
                    {
                        "image": xarray.DataArray(
                            xarray.Variable(
                                data=dask.array.from_array(data["medium_image"]),
                                dims=["x", "y"],
                                attrs={"thing": "stuff"},
                            ),
                            coords={
                                "x": dask.array.arange(len(data["medium_image"])) / 10,
                                "y": dask.array.arange(len(data["medium_image"])) / 50,
                            },
                        ),
                        "z": xarray.DataArray(
                            data=dask.array.ones((len(data["medium_image"]),))
                        ),
                    }
                )
            ),
            "xarray_data_array": DataArrayAdapter(
                xarray.DataArray(
                    xarray.Variable(
                        data=dask.array.from_array(data["medium_image"]),
                        dims=["x", "y"],
                        attrs={"thing": "stuff"},
                    )
                )
            ),
            "xarray_variable": VariableAdapter(
                xarray.Variable(
                    data=dask.array.from_array(data["medium_image"]),
                    dims=["x", "y"],
                    attrs={"thing": "stuff"},
                )
            ),
        },
        metadata={"animal": "cat", "color": "green"},
    ),
    # Below, an asynchronous task modifies this value over time.
    "dynamic": ArrayAdapter.from_array(numpy.zeros((3, 3))),
}
tree = Tree(mapping)


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
