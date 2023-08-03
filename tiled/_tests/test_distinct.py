import uuid

import numpy as np
import pandas as pd
import pytest

from ..adapters.array import ArrayAdapter
from ..adapters.dataframe import DataFrameAdapter
from ..adapters.mapping import MapAdapter
from ..client import Context, from_context
from ..queries import Key
from ..server.app import build_app

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

# Added additional field in metadata to implement consecutive search and distinct queries
for i in range(10):
    if i < 5:
        group = "A"
    else:
        group = "B"

    if i % 2 == 0:
        subgroup = "even"
        specs = ["MyDataFrame", "ExtendedSpec"]
    else:
        subgroup = "odd"
        specs = ["MyDataFrame"]

    if i == 0:
        tag = "Zero"
    else:
        for j in range(2, int(i / 2) + 1):
            if (i % j) == 0:
                tag = "NotPrime"
                break
        else:
            tag = "Prime"

    mapping[str(uuid.uuid4())] = DataFrameAdapter.from_pandas(
        pd.DataFrame({"a": np.ones(10)}),
        metadata={"group": group, "subgroup": subgroup, "tag": tag},
        specs=specs,
        npartitions=1,
    )

tree = MapAdapter(mapping)


@pytest.fixture(scope="module")
def context():
    app = build_app(tree)
    with Context.from_app(app) as context:
        yield context


def test_distinct(context):
    client = from_context(context)
    # test without counts
    distinct = client.distinct(
        "foo.bar", structure_families=True, specs=True, counts=False
    )
    expected = {
        "metadata": {"foo.bar": [{"value": v, "count": None} for v in values]},
        "specs": [
            {"value": [], "count": None},
            {"value": ["MyArray"], "count": None},
            {"value": ["MyDataFrame", "ExtendedSpec"], "count": None},
            {"value": ["MyDataFrame"], "count": None},
        ],
        "structure_families": [
            {"value": "container", "count": None},
            {"value": "array", "count": None},
            {"value": "table", "count": None},
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
            {"value": ["MyDataFrame", "ExtendedSpec"], "count": 5},
            {"value": ["MyDataFrame"], "count": 5},
        ],
        "structure_families": [
            {"value": "container", "count": 22},
            {"value": "array", "count": 10},
            {"value": "table", "count": 10},
        ],
    }

    assert distinct["metadata"] == expected["metadata"]
    assert distinct["specs"] == expected["specs"]
    assert distinct["structure_families"] == expected["structure_families"]

    # test with no matches
    distinct = client.distinct("baz", counts=True)
    expected = {"baz": []}
    assert distinct["metadata"] == expected


def test_search_distinct(context):
    client = from_context(context)
    distinct = (
        client.search(Key("group") == "A")
        .search(Key("subgroup") == "odd")
        .distinct("tag", counts=True)
    )

    expected = {
        "metadata": {
            "tag": [
                {"value": "Prime", "count": 2},
            ],
        },
    }

    assert distinct["metadata"] == expected["metadata"]
