import uuid

import numpy as np
import pandas as pd

from ..adapters.array import ArrayAdapter
from ..adapters.dataframe import DataFrameAdapter
from ..adapters.mapping import MapAdapter
from ..client import from_tree

values = ["a", "b", "c"]
counts = [10, 5, 7]

mapping = {}

for value, count in zip(values, counts):
    for _ in range(count):
        mapping[str(uuid.uuid4())] = MapAdapter({}, metadata={"foo": {"bar": value}})

# items which do not contain the queries metadata should not effect the results
for _ in range(10):
    mapping[str(uuid.uuid4())] = ArrayAdapter.from_array(
        np.ones(10), metadata={}, specs=["MyArray"]
    )

for _ in range(10):
    mapping[str(uuid.uuid4())] = DataFrameAdapter.from_pandas(
        pd.DataFrame({"a": np.ones(10)}),
        metadata={},
        specs=["MyDataFrame"],
        npartitions=1,
    )

tree = MapAdapter(mapping)
client = from_tree(tree)


def test_distinct():
    # test without counts
    distinct = client.distinct(
        "foo.bar", structure_families=True, specs=True, counts=False
    )
    expected = {
        "metadata": {"foo.bar": [{"value": v, "count": None} for v in values]},
        "specs": [
            {"value": [], "count": None},
            {"value": ["MyArray"], "count": None},
            {"value": ["MyDataFrame"], "count": None},
        ],
        "structure_families": [
            {"value": "node", "count": None},
            {"value": "array", "count": None},
            {"value": "dataframe", "count": None},
        ],
    }

    assert distinct["metadata"] == expected["metadata"]
    assert distinct["specs"] == expected["specs"]
    assert distinct["structure_families"] == expected["structure_families"]

    # test with counts
    distinct = client.distinct(
        "foo.bar", structure_families=True, specs=True, counts=True
    )
    expected = {
        "metadata": {
            "foo.bar": [{"value": v, "count": c} for v, c in zip(values, counts)]
        },
        "specs": [
            {"value": [], "count": 22},
            {"value": ["MyArray"], "count": 10},
            {"value": ["MyDataFrame"], "count": 10},
        ],
        "structure_families": [
            {"value": "node", "count": 22},
            {"value": "array", "count": 10},
            {"value": "dataframe", "count": 10},
        ],
    }

    assert distinct["metadata"] == expected["metadata"]
    assert distinct["specs"] == expected["specs"]
    assert distinct["structure_families"] == expected["structure_families"]

    # test with no matches
    distinct = client.distinct("baz", counts=True)
    expected = {"baz": []}
    assert distinct["metadata"] == expected
