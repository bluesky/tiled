import numpy
import pandas
import pandas.testing
import pytest
import awkward
import sparse

from ..catalog import in_memory
from ..client import Context, from_context
from ..server.app import build_app
from ..structures.array import ArrayStructure
from ..structures.core import StructureFamily
from ..structures.data_source import DataSource
from ..structures.table import TableStructure
from ..structures.awkward import AwkwardStructure
from ..structures.sparse import COOStructure

rng = numpy.random.default_rng(12345)

df1 = pandas.DataFrame({"A": ["one", "two", "three"], "B": [1, 2, 3]})
df2 = pandas.DataFrame(
    {
        "C": ["red", "green", "blue", "white"],
        "D": [10.0, 20.0, 30.0, 40.0],
        "E": [0, 0, 0, 0],
    }
)
arr1 = rng.random(size=(3, 5), dtype="float64")
arr2 = rng.integers(0, 255, size=(5, 7, 3), dtype="uint8")

# An awkward array
awk_arr = awkward.Array(
    [
        [{"x": 1.1, "y": [1]}, {"x": 2.2, "y": [1, 2]}],
        [],
        [{"x": 3.3, "y": [1, 2, 3]}],
    ]
)
awk_packed = awkward.to_packed(awk_arr)
awk_form, awk_length, awk_container = awkward.to_buffers(awk_packed)

# A sparse array
arr = rng.random(size=(10, 20, 30), dtype="float64")
arr[arr < 0.95] = 0  # Fill half of the array with zeros.
sps_arr = sparse.COO(arr)

md = {"md_key1": "md_val1", "md_key2": 2}

@pytest.fixture(scope="module")
def tree(tmp_path_factory):
    return in_memory(writable_storage=tmp_path_factory.getbasetemp())


@pytest.fixture(scope="module")
def context(tree):
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)
        x = client.create_consolidated(
            [
                DataSource(
                    structure_family=StructureFamily.table,
                    structure=TableStructure.from_pandas(df1),
                    name="table1",
                ),
                DataSource(
                    structure_family=StructureFamily.table,
                    structure=TableStructure.from_pandas(df2),
                    name="table2",
                ),
                DataSource(
                    structure_family=StructureFamily.array,
                    structure=ArrayStructure.from_array(arr1),
                    name="A1",
                ),
                DataSource(
                    structure_family=StructureFamily.array,
                    structure=ArrayStructure.from_array(arr2),
                    name="A2",
                ),
                DataSource(
                    structure_family=StructureFamily.awkward,
                    structure=AwkwardStructure(
                                length=awk_length,
                                form=awk_form.to_dict(),
                            ),
                    name="AWK",
                ),
                DataSource(
                    structure_family=StructureFamily.sparse,
                    structure=COOStructure(shape=sps_arr.shape,
                                        chunks=tuple((dim,) for dim in sps_arr.shape)),
                    name="SPS",
                ),
            ],
            key="x",
            metadata=md,
        )
        # Write by data source.
        x.parts["table1"].write(df1)
        x.parts["table2"].write(df2)
        x.parts["A1"].write_block(arr1, (0, 0))
        x.parts["A2"].write_block(arr2, (0, 0, 0))
        x.parts["AWK"].write(awk_container)
        x.parts["SPS"].write(coords=sps_arr.coords, data=sps_arr.data)

        yield context

@pytest.mark.parametrize(
    "name, expected",
    [
        ("A", df1["A"]),
        ("B", df1["B"]),
        ("C", df2["C"]),
        ("D", df2["D"]),
        ("E", df2["E"]),
        ("A1", arr1),
        ("A2", arr2),
        ("AWK", awk_arr),
        ("SPS", sps_arr.todense()),
    ],
)
def test_reading(context, name, expected):
    client = from_context(context)
    actual = client["x"][name].read()
    if name == "SPS":
        actual = actual.todense()
    assert numpy.array_equal(actual, expected)


def test_iterate_parts(context):
    client = from_context(context)
    for part in client["x"].parts:
        client["x"].parts[part].read()


def test_iterate_columns(context):
    client = from_context(context)
    for col in client["x"]:
        client["x"][col].read()
        client[f"x/{col}"].read()


def test_metadata(context):
    client = from_context(context)
    assert client["x"].metadata == md

