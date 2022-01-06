import numpy
import pandas

from tiled.adapters.array import ArrayAdapter
from tiled.adapters.dataframe import DataFrameAdapter
from tiled.adapters.mapping import MapAdapter

tree = MapAdapter(
    {
        "A": ArrayAdapter.from_array(numpy.ones((100, 100))),
        "B": ArrayAdapter.from_array(numpy.ones((100, 100, 100))),
        "C": DataFrameAdapter.from_pandas(
            pandas.DataFrame(
                {
                    "x": 1 * numpy.ones(100),
                    "y": 2 * numpy.ones(100),
                    "z": 3 * numpy.ones(100),
                }
            ),
            npartitions=3,
        ),
    },
    metadata={"thing": "stuff"},
)
