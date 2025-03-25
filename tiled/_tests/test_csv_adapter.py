from pathlib import Path

import numpy
import pandas
import pytest

from ..adapters.csv import CSVArrayAdapter
from ..catalog import in_memory
from ..client import Context, from_context
from ..server.app import build_app
from ..structures.array import ArrayStructure
from ..structures.core import StructureFamily
from ..structures.data_source import Asset, DataSource, Management
from ..structures.table import TableStructure

rng = numpy.random.default_rng(12345)

df1 = pandas.DataFrame(
    {
        "C": ["red", "green", "blue", "white"],
        "D": [10.0, 20.0, 30.0, 40.0],
        "E": [0, 1, 2, 3],
    }
)

arr1 = rng.integers(0, 255, size=(13, 17), dtype="uint8")
arr2 = rng.random(size=(15, 19), dtype="float64")
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
def csv_table_file(tmpdir):
    fpath = Path(tmpdir, "table.csv")
    df1.to_csv(fpath, index=False)

    yield str(fpath)


@pytest.fixture
def csv_array1_file(tmpdir):
    fpath = Path(tmpdir, "array_1.csv")
    df_arr1.to_csv(fpath, index=False, header=False)

    yield str(fpath)


@pytest.fixture
def csv_array2_file(tmpdir):
    fpath = Path(tmpdir, "array_2.csv")
    df_arr2.to_csv(fpath, index=False, header=False)

    yield str(fpath)


def test_csv_table(context, csv_table_file):
    client = from_context(context)

    csv_assets = [
        Asset(
            data_uri=f"file://localhost/{csv_table_file}",
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


def test_csv_arrays(context, csv_array1_file, csv_array2_file):
    client = from_context(context)

    for key, csv_fpath, arr in zip(
        ("array1", "array2"), (csv_array1_file, csv_array2_file), (arr1, arr2)
    ):
        csv_assets = [
            Asset(
                data_uri=f"file://localhost/{csv_fpath}",
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


def test_csv_arrays_from_uris(csv_array1_file, csv_array2_file):
    array_adapter = CSVArrayAdapter.from_uris(f"file://localhost/{csv_array1_file}")
    read_arr = array_adapter.read()
    assert numpy.isclose(read_arr, arr1).all()

    array_adapter = CSVArrayAdapter.from_uris(f"file://localhost/{csv_array2_file}")
    read_arr = array_adapter.read()
    assert numpy.isclose(read_arr, arr2).all()
