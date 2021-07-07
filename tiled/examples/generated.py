import sys

import dask.array
import dask.dataframe
import numpy
import pandas
import xarray

from tiled.readers.array import ArrayAdapter
from tiled.readers.dataframe import DataFrameAdapter
from tiled.readers.xarray import DataArrayAdapter, DatasetAdapter, VariableAdapter
from tiled.trees.in_memory import Tree


print("Generating large example data...", file=sys.stderr)
array_data = {
    "large": numpy.random.random((10_000, 10_000)),
    "medium": numpy.random.random((1000, 1000)),
    "small": numpy.random.random((10, 10)),
    "tiny": numpy.random.random((3, 3)),
}
A = numpy.random.random(100)
B = numpy.random.random(100)
C = numpy.random.random(100)


arrays = Tree({name: ArrayAdapter.from_array(arr) for name, arr in array_data.items()})

dataframes = Tree(
    {
        "df": DataFrameAdapter.from_pandas(
            pandas.DataFrame(
                {
                    "A": A,
                    "B": B,
                    "C": C,
                },
                index=pandas.Index(numpy.arange(100), name="index"),
            ),
            npartitions=3,  # Partition for demo purposes, even though it's small.
        )
    }
)
xarrays = Tree(
    {
        name: Tree(
            {
                "variable": VariableAdapter(
                    xarray.Variable(
                        data=dask.array.from_array(array),
                        dims=["x", "y"],
                        attrs={"thing": "stuff"},
                    ),
                ),
                "data_array": DataArrayAdapter(
                    xarray.DataArray(
                        xarray.Variable(
                            data=dask.array.from_array(array),
                            dims=["x", "y"],
                            attrs={"thing": "stuff"},
                        ),
                        coords={
                            "x": dask.array.arange(len(array)),
                            "y": 10 * dask.array.arange(len(array)),
                        },
                    ),
                ),
                "dataset": DatasetAdapter(
                    xarray.Dataset(
                        {
                            "image": xarray.DataArray(
                                xarray.Variable(
                                    data=dask.array.from_array(array),
                                    dims=["x", "y"],
                                    attrs={"thing": "stuff"},
                                ),
                                coords={
                                    "x": dask.array.arange(len(array)),
                                    "y": 10 * dask.array.arange(len(array)),
                                },
                            ),
                            "z": xarray.DataArray(data=dask.array.ones((len(array),))),
                        }
                    )
                ),
            }
        )
        for name, array in array_data.items()
    },
    metadata={"description": "the three main xarray data structures"},
)


# Build nested Tree of Trees.
subtrees = {}
for name, fruit, animal in zip(
    ["tiny", "small", "medium", "large"],
    ["apple", "banana", "orange", "grape"],
    ["bird", "cat", "dog", "penguin"],
):
    subtrees[name] = Tree(
        {
            inner_name: ArrayAdapter.from_array(10 ** exponent * array_data[name])
            for inner_name, exponent in zip(["ones", "tens", "hundreds"], [0, 1, 2])
        },
        metadata={"fruit": fruit, "animal": animal},
    )
nested = Tree(subtrees)


# This a bit contrived, the same subtree used three times.
very_nested = Tree({"a": nested, "b": nested, "c": nested})

demo = Tree(
    {
        "arrays": arrays,
        "dataframes": dataframes,
        "xarrays": xarrays,
        "nested": nested,
        "very_nested": very_nested,
    }
)

print("Done generating example data.", file=sys.stderr)
