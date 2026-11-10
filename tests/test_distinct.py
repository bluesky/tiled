import uuid

import numpy as np
import pandas as pd
import pytest

from tiled.adapters.array import ArrayAdapter
from tiled.adapters.dataframe import DataFrameAdapter
from tiled.adapters.mapping import MapAdapter
from tiled.catalog import in_memory
from tiled.client import Context, from_context
from tiled.queries import Key
from tiled.server.app import build_app

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


# Catalog-backed tree. Every node has `m` so no missing-value rows blur the counts.
#
# outside      m="outside"
# a/           m="a"
#   inside     m="inside"
#   b/         m="b"
#     deep     m="deep"
@pytest.fixture(scope="module")
def catalog_client(tmp_path_factory):
    catalog = in_memory(
        writable_storage=str(tmp_path_factory.mktemp("distinct")),
        named_memory=f"distinct_{uuid.uuid4().hex}",
    )
    with Context.from_app(build_app(catalog)) as context:
        client = from_context(context)
        client.write_array(np.ones(2), key="outside", metadata={"m": "outside"})
        a = client.create_container("a", metadata={"m": "a"})
        a.write_array(np.ones(2), key="inside", metadata={"m": "inside"})
        b = a.create_container("b", metadata={"m": "b"})
        b.write_array(np.ones(2), key="deep", metadata={"m": "deep"})
        yield client


def _values(distinct, key="m"):
    return sorted(
        (item["value"], item["count"])
        for item in distinct["metadata"][key]
        if item["value"] is not None
    )


def test_distinct_is_scoped_to_the_requested_node(catalog_client):
    "Only the children of the requested node are counted, not the whole catalog."
    assert _values(catalog_client.distinct("m", counts=True)) == [
        ("a", 1),
        ("outside", 1),
    ]
    assert _values(catalog_client["a"].distinct("m", counts=True)) == [
        ("b", 1),
        ("inside", 1),
    ]
    assert _values(catalog_client["a"]["b"].distinct("m", counts=True)) == [
        ("deep", 1),
    ]


def test_distinct_structure_families_and_specs_are_scoped(catalog_client):
    distinct = catalog_client["a"].distinct(
        "m", structure_families=True, specs=True, counts=True
    )
    # `a` holds one array (`inside`) and one container (`b`); `outside` is excluded.
    assert sorted((i["value"], i["count"]) for i in distinct["structure_families"]) == [
        ("array", 1),
        ("container", 1),
    ]
    assert sum(i["count"] for i in distinct["specs"]) == 2


async def test_distinct_on_deep_variation_covers_all_descendants(catalog_client):
    "Deep scope counts every descendant of the node, and nothing outside it."
    catalog = catalog_client.context.app.state.root_tree
    a = await catalog.lookup_adapter(["a"])
    distinct = await a.search_deep().get_distinct(["m"], False, False, True)
    assert _values(distinct) == [("b", 1), ("deep", 1), ("inside", 1)]
