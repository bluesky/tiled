import numpy
import xarray

from tiled.adapters.array import ArrayAdapter
from tiled.adapters.dataframe import TableAdapter
from tiled.adapters.mapping import MapAdapter
from tiled.adapters.xarray import DatasetAdapter

tree = MapAdapter(
    {
        "A": ArrayAdapter.from_array(numpy.ones((100, 100))),
        "B": ArrayAdapter.from_array(numpy.ones((100, 100, 100))),
        "C": TableAdapter.from_dict(
            {
                "x": 1 * numpy.ones(100),
                "y": 2 * numpy.ones(100),
                "z": 3 * numpy.ones(100),
            },
            npartitions=3,
        ),
        "D": DatasetAdapter.from_dataset(
            xarray.Dataset(
                data_vars={"temperature": ("time", [100, 99, 98])},
                coords={"time": [1, 2, 3]},
            )
        ),
    },
    metadata={"thing": "stuff"},
)
