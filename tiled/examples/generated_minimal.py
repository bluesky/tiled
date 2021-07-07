from datetime import datetime
import numpy
import pandas
from tiled.trees.in_memory import Tree
from tiled.readers.array import ArrayAdapter
from tiled.readers.dataframe import DataFrameAdapter


tree = Tree(
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
    metadata={"d": datetime.now()},
)
