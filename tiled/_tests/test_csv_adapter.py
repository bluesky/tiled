from pathlib import Path

import numpy
import pandas
import pytest

from ..adapters.csv import CSVArrayAdapter
from ..catalog import in_memory
from ..client import Context, from_context
from ..server.app import build_app
from ..structures.array import ArrayStructure, StructDtype
from ..structures.core import StructureFamily
from ..structures.data_source import Asset, DataSource, Management
from ..structures.table import TableStructure
from ..utils import ensure_uri

rng = numpy.random.default_rng(12345)

df1 = pandas.DataFrame(
    {
        "A": ["red", "green", "blue", "white"],
        "B": [10.0, 20.0, 30.0, 40.0],
        "C": [0, 1, 2, 3],
    }
)

arr1 = rng.integers(0, 255, size=(13, 17), dtype="uint8")
arr2 = rng.random(size=(15, 19), dtype="float64")
arr3 = rng.random(size=(15, 19), dtype="float32")
df_arr1 = pandas.DataFrame(arr1)
df_arr2 = pandas.DataFrame(arr2)


@pytest.fixture(scope="module")
def tree(tmp_path_factory):
    return in_memory(writable_storage=tmp_path_factory.getbasetemp())


@pytest.fixture(scope="module")
def context(tree):
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)
        client.create_container(key="x")
        yield context


@pytest.fixture
def csv_table_uri(tmpdir):
    fpath = Path(tmpdir, "table.csv")
    df1.to_csv(fpath, index=False)

    yield ensure_uri(fpath)


@pytest.fixture
def csv_array1_uri(tmpdir):
    fpath = Path(tmpdir, "array_1.csv")
    df_arr1.to_csv(fpath, index=False, header=False)

    yield ensure_uri(fpath)


@pytest.fixture
def csv_array2_uri(tmpdir):
    fpath = Path(tmpdir, "array_2.csv")
    df_arr2.to_csv(fpath, index=False, header=False)

    yield ensure_uri(fpath)


def test_csv_table(context, csv_table_uri):
    client = from_context(context)

    csv_assets = [
        Asset(
            data_uri=csv_table_uri,
            is_directory=False,
            parameter="data_uris",
            num=0,
        )
    ]
    csv_data_source = DataSource(
        mimetype="text/csv;header=present",
        assets=csv_assets,
        structure_family=StructureFamily.table,
        structure=TableStructure.from_pandas(df1),
        management=Management.external,
    )

    client["x"].new(
        structure_family=StructureFamily.table,
        data_sources=[csv_data_source],
        key="table",
    )

    read_df = client["x"]["table"].read()
    assert set(read_df.columns) == set(df1.columns)
    assert (read_df == df1).all().all()


def test_csv_struct_dtype_array(context, csv_table_uri):
    # Test reading a CSV table as a struct-dtyped array
    client = from_context(context)

    numpy_data_type = numpy.dtype([("A", "<U8"), ("B", "<f8"), ("C", "<i8")])
    arr = df1.to_records(index=False).astype(numpy_data_type)
    structure = ArrayStructure(
        data_type=StructDtype.from_numpy_dtype(numpy_data_type),
        shape=(len(arr), 1),  # the adapter should force the shape to be 2D
        chunks=((len(arr),), (1,)),
    )
    csv_assets = [
        Asset(
            data_uri=csv_table_uri,
            is_directory=False,
            parameter="data_uris",
            num=0,
        )
    ]
    csv_data_source = DataSource(
        mimetype="text/csv;header=absent",  # ignore the header -- it is an "array"
        assets=csv_assets,
        parameters={"skiprows": 1, "header": None},  # skip the header row
        structure_family=StructureFamily.array,
        structure=structure,
        management=Management.external,
    )

    client["x"].new(
        structure_family=StructureFamily.array,
        data_sources=[csv_data_source],
        key="struct_array",
    )

    read_arr = client["x"]["struct_array"].read()

    assert read_arr.shape == (4, 1)
    assert (read_arr.ravel() == arr.ravel()).all()


def test_csv_arrays(context, csv_array1_uri, csv_array2_uri):
    client = from_context(context)

    for key, data_uri, arr in zip(
        ("array1", "array2"), (csv_array1_uri, csv_array2_uri), (arr1, arr2)
    ):
        csv_assets = [
            Asset(
                data_uri=data_uri,
                is_directory=False,
                parameter="data_uris",
                num=0,
            )
        ]
        csv_data_source = DataSource(
            mimetype="text/csv;header=absent",
            assets=csv_assets,
            structure_family=StructureFamily.array,
            structure=ArrayStructure.from_array(arr),
            management=Management.external,
        )

        client["x"].new(
            structure_family=StructureFamily.array,
            data_sources=[csv_data_source],
            key=key,
        )

    read_arr1 = client["x"]["array1"].read()
    assert numpy.array_equal(read_arr1, arr1)

    read_arr2 = client["x"]["array2"].read()
    assert numpy.isclose(read_arr2, arr2).all()


def test_csv_arrays_from_uris(csv_array1_uri, csv_array2_uri):
    array_adapter = CSVArrayAdapter.from_uris(csv_array1_uri)
    read_arr = array_adapter.read()
    assert numpy.isclose(read_arr, arr1).all()

    array_adapter = CSVArrayAdapter.from_uris(csv_array2_uri)
    read_arr = array_adapter.read()
    assert numpy.isclose(read_arr, arr2).all()
