import numpy
import pandas
import pandas.testing
import pytest

from ..catalog import in_memory
from ..client import Context, from_context
from ..server.app import build_app
from ..structures.array import ArrayStructure
from ..structures.core import StructureFamily
from ..structures.data_source import DataSource
from ..structures.table import TableStructure

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
                    name="F",
                ),
                DataSource(
                    structure_family=StructureFamily.array,
                    structure=ArrayStructure.from_array(arr2),
                    name="G",
                ),
            ],
            key="x",
            metadata=md,
        )
        # Write by data source.
        x.parts["table1"].write(df1)
        x.parts["table2"].write(df2)
        x.parts["F"].write_block(arr1, (0, 0))
        x.parts["G"].write_block(arr2, (0, 0, 0))

        yield context


def test_iterate_parts(context):
    client = from_context(context)
    for part in client["x"].parts:
        client["x"].parts[part].read()


def test_iterate_columns(context):
    client = from_context(context)
    for col in client["x"]:
        if col not in ("A", "C"):
            # TODO: reading string columns raises TypeError: Cannot interpret 'string[pyarrow]' as a data type
            client["x"][col].read()
            client[f"x/{col}"].read()


def test_metadata(context):
    client = from_context(context)
    assert client["x"].metadata == md
