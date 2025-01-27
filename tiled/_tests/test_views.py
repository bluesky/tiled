from pathlib import Path

import awkward
import numpy
import pandas
import pytest
import sparse
import tifffile as tf

from ..catalog import in_memory
from ..client import Context, from_context
from ..server.app import build_app
from ..structures.array import ArrayStructure, BuiltinDtype
from ..structures.core import StructureFamily
from ..structures.data_source import Asset, DataSource, Management
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
df3 = pandas.DataFrame(
    {
        "col1": ["one", "two", "three", "four", "five"],
        "col2": [1.0, 2.0, 3.0, 4.0, 5.0],
    }
)
arr1 = rng.random(size=(13, 15), dtype="float64")
arr2 = rng.integers(0, 255, size=(5, 7, 3), dtype="uint8")
img_data = rng.integers(0, 255, size=(5, 13, 17, 3), dtype="uint8")

md = {"md_key1": "md_val1", "md_key2": 2}

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
def csv_file(tmpdir):
    fpath = Path(tmpdir, "test.csv")
    df3.to_csv(fpath, index=False)

    yield fpath


@pytest.fixture(scope="module")
def tree(tmp_path_factory):
    return in_memory(writable_storage=tmp_path_factory.getbasetemp())


@pytest.fixture(scope="module")
def context(tree):
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)

        # Write data in the root container
        client.write_array(arr1, key="arr1", metadata={"md_key": "md_for_arr1"})
        client.write_dataframe(df1, key="df1", metadata={"md_key": "md_for_df1"})

        # Write data in subcontainers
        x = client.create_container(key="x", metadata=md)
        x.write_array(arr1, key="arr1", metadata={"md_key": "md_for_arr1"})
        x.write_array(arr2, key="arr2", metadata={"md_key": "md_for_arr2"})
        x.write_dataframe(df1, key="df1", metadata={"md_key": "md_for_df1"})
        x.write_dataframe(df2, key="df2", metadata={"md_key": "md_for_df2"})
        y = x.create_container(key="y", metadata=md)
        y.write_array(arr1, key="arr1", metadata={"md_key": "md_for_arr1"})
        y.write_dataframe(df1, key="df1", metadata={"md_key": "md_for_df1"})

        yield context


def test_original_locations(context):
    client = from_context(context)
    arr_v = client.create_container(key = 'arr_v')
    arr_v.create_array_view(links=['/arr1'], key='arr1')
    arr_v.create_array_view(links=['/x/arr1'], key='x_arr1')
    arr_v.create_array_view(links=['/x/y/arr1'], key='x_y_arr1')

    for key in ('arr1', 'x_arr1', 'x_y_arr1'):
        assert numpy.array_equal(arr_v[key].read(), arr1)


def test_table_columns(context):
    client = from_context(context)
    tbl_v = client.create_container(key = 'tbl_v')
    tbl_v.create_array_view(links=['/x/y/df1/A'], key='A')
    tbl_v.create_array_view(links=['/x/y/df1/B'], key='B')

    for key in ('A', 'B'):
        assert numpy.array_equal(tbl_v[key].read(), df1[key])

def test_slices(context):
    client = from_context(context)
    slc_v = client.create_container(key = 'slc_v')
    slc_v.create_array_view(links=['/x/df2/C'], key='C', slices=[(slice(0, 2),)])
    assert numpy.array_equal(slc_v['C'].read(), df2['C'][0:2])

    slc_v.create_array_view(links=['/x/arr2'], key='a2_v', slices=[(slice(0, 2), 1, ...)])
    assert numpy.array_equal(slc_v['a2_v'].read(), arr2[slice(0, 2), 1, ...])


def test_external_assets(context, tiff_sequence, csv_file):
    client = from_context(context)

    # Write some data with external assets
    tiff_assets = [
        Asset(
            data_uri=f"file://localhost{fpath}",
            is_directory=False,
            parameter="data_uris",
            num=i + 1,
        )
        for i, fpath in enumerate(tiff_sequence)
    ]
    tiff_structure_0 = ArrayStructure(
        data_type=BuiltinDtype.from_numpy_dtype(numpy.dtype("uint8")),
        shape=(5, 13, 17, 3),
        chunks=((1, 1, 1, 1, 1), (13,), (17,), (3,)),
    )
    tiff_data_source = DataSource(
        mimetype="multipart/related;type=image/tiff",
        assets=tiff_assets,
        structure_family=StructureFamily.array,
        structure=tiff_structure_0,
        management=Management.external,
    )

    csv_assets = [
        Asset(
            data_uri=f"file://localhost{csv_file}",
            is_directory=False,
            parameter="data_uris",
        )
    ]
    csv_data_source = DataSource(
        mimetype="text/csv",
        assets=csv_assets,
        structure_family=StructureFamily.table,
        structure=TableStructure.from_pandas(df3),
        management=Management.external,
    )

    z = client.create_container(key="z")
    z.new(
        structure_family=StructureFamily.array,
        data_sources=[tiff_data_source],
        key="image",
    )
    z.new(
        structure_family=StructureFamily.table,
        data_sources=[csv_data_source],
        key="table",
    )

    ext_v = client.create_container(key = 'ext_v')
    ext_v.create_array_view(links=['/z/image'], key='image_v')
    ext_v.create_array_view(links=['/z/table/col1'], key='col1_v')
    ext_v.create_array_view(links=['/z/table/col2'], key='col2_v')
    assert numpy.array_equal(ext_v['image_v'].read(), img_data)
    assert numpy.array_equal(ext_v['col1_v'].read(), df3['col1'])
    assert numpy.array_equal(ext_v['col2_v'].read(), df3['col2'])
