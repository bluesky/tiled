from pathlib import Path

import awkward
import numpy
import pandas
import pytest
import sparse
import tifffile as tf

from ...catalog import in_memory
from ...client import Context, from_context
from ...server.app import build_app
from ...structures.array import ArrayStructure, BuiltinDtype
from ...structures.core import StructureFamily
from ...structures.data_source import Asset, DataSource, Management
from ...structures.table import TableStructure

rng = numpy.random.default_rng(12345)

df1 = pandas.DataFrame(
    {
        "C": ["red", "green", "blue", "white"],
        "D": [10.0, 20.0, 30.0, 40.0],
        "E": [0, 1, 2, 3],
    }
)

arr1 = rng.integers(0, 255, size=(13, 17), dtype="uint8")
arr2 = rng.random(size=(15, 19), dtype='float64')
df_arr1 = pandas.DataFrame(arr1)
df_arr2 = pandas.DataFrame(arr2)


@pytest.fixture(scope="module")
def tree(tmp_path_factory):
    return in_memory(writable_storage=tmp_path_factory.getbasetemp())


@pytest.fixture(scope="module")
def context(tree):
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)
        x = client.create_container(key="x")
        # x.write_array(arr1, key="arr1", metadata={"md_key": "md_for_arr1"})
        # x.write_array(arr2, key="arr2", metadata={"md_key": "md_for_arr2"})
        # x.write_dataframe(df1, key="df1", metadata={"md_key": "md_for_df1"})
        # x.write_dataframe(df2, key="df2", metadata={"md_key": "md_for_df2"})

        yield context


@pytest.fixture
def tiff_sequence(tmpdir):
    sequence_directory = Path(tmpdir, "sequence")
    sequence_directory.mkdir()
    filepaths = []
    for i in range(img_data.shape[0]):
        fpath = sequence_directory / f"temp{i:05}.tif"
        tf.imwrite(fpath, img_data[i, ...])
        filepaths.append(fpath)

    yield filepaths


@pytest.fixture
def csv_table_file(tmpdir):
    fpath = Path(tmpdir, "table.csv")
    df1.to_csv(fpath, index=False)

    yield fpath

@pytest.fixture
def csv_array1_file(tmpdir):
    fpath = Path(tmpdir, "array_1.csv")
    df_arr1.to_csv(fpath, index=False, header=False)

    yield fpath

@pytest.fixture
def csv_array2_file(tmpdir):
    fpath = Path(tmpdir, "array_2.csv")
    df_arr2.to_csv(fpath, index=False, header=False)

    yield fpath


def test_csv_table(context, csv_table_file):
    client = from_context(context)

    csv_assets = [
        Asset(
            data_uri=f"file://localhost{csv_table_file}",
            is_directory=False,
            parameter="data_uris",
        )
    ]
    csv_data_source = DataSource(
        mimetype="text/csv;header=present",
        assets=csv_assets,
        structure_family=StructureFamily.table,
        structure=TableStructure.from_pandas(df1),
        management=Management.external,
    )

    client['x'].new(
        structure_family=StructureFamily.table,
        data_sources=[csv_data_source],
        key="table",
    )

    read_df = client['x']['table'].read()
    assert set(read_df.columns) == set(df1.columns)
    assert (read_df == df1).all().all()


def test_csv_arrays(context, csv_array1_file, csv_array2_file):
    client = from_context(context)

    for key, csv_fpath, arr in zip(('array1', 'array2'), (csv_array1_file, csv_array2_file), (arr1, arr2)):

        csv_assets = [
            Asset(
                data_uri=f"file://localhost{csv_fpath}",
                is_directory=False,
                parameter="data_uris",
            )
        ]
        csv_data_source = DataSource(
            mimetype="text/csv;header=absent",
            assets=csv_assets,
            structure_family=StructureFamily.array,
            structure=ArrayStructure.from_array(arr),
            management=Management.external,
        )

        client['x'].new(
            structure_family=StructureFamily.array,
            data_sources=[csv_data_source],
            key=key,
        )

    read_arr1 = client['x']['array1'].read()
    assert numpy.array_equal(read_arr1, arr1)

    read_arr2 = client['x']['array2'].read()
    assert numpy.isclose(read_arr2, arr2).all()
