from pathlib import Path

import dask.array
import h5py
import pandas
import xarray

from ..readers.array import ArrayReader
from ..readers.dataframe import DataFrameReader
from ..readers.xarray import DataArrayReader, DatasetReader, VariableReader
from ..catalogs.in_memory import Catalog, SimpleAccessPolicy
from ..utils import SpecialUsers


def access_hdf5_data(name, inner_name, value, size):
    path = Path("example_data", "hdf5")
    filename = f"{name}_{inner_name}.h5"
    file = h5py.File(path / filename, "r")
    return file["data"]


arrays = Catalog(
    {
        name: ArrayReader(access_hdf5_data(name, "ones", 1, size))
        for name, size in zip(
            ["tiny", "small", "medium", "large"],
            [3, 100, 1000, 10_000],
        )
    }
)

arr = access_hdf5_data("tiny", "ones", 1, 3)
dataframes = Catalog(
    {"df": DataFrameReader(pandas.DataFrame({"A": arr[0], "B": arr[1], "C": arr[2]}))}
)
xarrays = Catalog(
    {
        name: Catalog(
            {
                "variable": VariableReader(
                    xarray.Variable(
                        data=dask.array.from_array(
                            access_hdf5_data(name, "ones", 1, size)
                        ),
                        dims=["x", "y"],
                        attrs={"thing": "stuff"},
                    ),
                ),
                "data_array": DataArrayReader(
                    xarray.DataArray(
                        xarray.Variable(
                            data=dask.array.from_array(
                                access_hdf5_data(name, "ones", 1, size)
                            ),
                            dims=["x", "y"],
                            attrs={"thing": "stuff"},
                        ),
                        coords={
                            "x": dask.array.arange(size),
                            "y": 10 * dask.array.arange(size),
                        },
                    ),
                ),
                "dataset": DatasetReader(
                    xarray.Dataset(
                        {
                            "image": xarray.DataArray(
                                xarray.Variable(
                                    data=dask.array.from_array(
                                        access_hdf5_data(name, "ones", 1, size)
                                    ),
                                    dims=["x", "y"],
                                    attrs={"thing": "stuff"},
                                ),
                                coords={
                                    "x": dask.array.arange(size),
                                    "y": 10 * dask.array.arange(size),
                                },
                            ),
                            "z": xarray.DataArray(data=dask.array.ones((size,))),
                        }
                    )
                ),
            }
        )
        for name, size in zip(
            ["tiny", "small", "medium", "large"],
            [3, 100, 1000, 10_000],
        )
    },
    metadata={"description": "the three main xarray data structures"},
)


# Build nested Catalog of Catalogs.
subcatalogs = {}
for name, size, fruit, animal in zip(
    ["tiny", "small", "medium", "large"],
    [3, 100, 1000, 10_000],
    ["apple", "banana", "orange", "grape"],
    ["bird", "cat", "dog", "penguin"],
):
    subcatalogs[name] = Catalog(
        {
            inner_name: ArrayReader(access_hdf5_data(name, inner_name, value, size))
            for inner_name, value in zip(["ones", "twos", "threes"], [1, 2, 3])
        },
        metadata={"fruit": fruit, "animal": animal},
    )
nested = Catalog(subcatalogs)


access_policy = SimpleAccessPolicy(
    {
        SpecialUsers.public: ["medium"],
        "alice": ["medium", "large"],
        "bob": ["tiny", "medium"],
        "cara": SimpleAccessPolicy.ALL,
    }
)
nested_with_access_control = Catalog(subcatalogs, access_policy=access_policy)


# This a bit contrived, the same subcatalog used three times.
very_nested = Catalog({"a": nested, "b": nested, "c": nested})

demo = Catalog(
    {
        "arrays": arrays,
        "dataframes": dataframes,
        "xarrays": xarrays,
        "nested": nested,
        # "nested_with_access_control": nested_with_access_control,
        "very_nested": very_nested,
    }
)
