from pathlib import Path

import awkward
import numpy
import pandas
import pytest
import sparse
import tifffile as tf
import xarray

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
arr1 = rng.random(size=(3, 5), dtype="float64")
arr2 = rng.integers(0, 255, size=(5, 7, 3), dtype="uint8")
img_data = rng.integers(0, 255, size=(5, 13, 17, 3), dtype="uint8")

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
sps_arr = sparse.COO(numpy.where(arr > 0.95, arr, 0))

md = {"md_key1": "md_val1", "md_key2": 2}


@pytest.fixture(scope="module")
def tree(tmp_path_factory):
    return in_memory(writable_storage=tmp_path_factory.getbasetemp())


@pytest.fixture(scope="module")
def context(tree):
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)
        x = client.create_composite(key="x", metadata=md)
        x.write_array(arr1, key="arr1", metadata={"md_key": "md_for_arr1"})
        x.write_array(arr2, key="arr2", metadata={"md_key": "md_for_arr2"})
        x.write_dataframe(
            df1,
            key="df1",
            metadata={
                "md_key": "md_for_df1",
                "A": {"md_key": "md_for_A"},
                "B": {"md_key": "md_for_B"},
            },
        )
        x.write_dataframe(
            df2,
            key="df2",
            metadata={
                "md_key": "md_for_df2",
                "C": {"md_key": "md_for_C"},
                "D": {"md_key": "md_for_D"},
                "E": {"md_key": "md_for_E"},
            },
        )
        x.write_awkward(awk_arr, key="awk", metadata={"md_key": "md_for_awk"})
        x.write_sparse(
            coords=sps_arr.coords,
            data=sps_arr.data,
            shape=sps_arr.shape,
            key="sps",
            metadata={"md_key": "md_for_sps"},
        )

        yield context


@pytest.fixture(scope="module")
def context_for_read(context):
    client = from_context(context)

    # Awkward arrays are not supported when building xarray, in general
    client["x"].delete("awk")
    # Add an image array and a table with 5 rows
    client["x"].write_array(img_data, key="img")
    client["x"].write_dataframe(df3, key="df3")

    yield context

    # Restore the original context
    client["x"].write_awkward(awk_arr, key="awk", metadata={"md_key": "md_for_awk"})
    client["x"].delete("img")
    client["x"].delete("df3")


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


@pytest.mark.parametrize(
    "name, expected",
    [
        ("A", df1["A"]),
        ("B", df1["B"]),
        ("C", df2["C"]),
        ("D", df2["D"]),
        ("E", df2["E"]),
        ("arr1", arr1),
        ("arr2", arr2),
        ("awk", awk_arr),
        ("sps", sps_arr.todense()),
    ],
)
def test_reading(context, name, expected):
    client = from_context(context)
    actual = client["x"][name].read()
    if name == "sps":
        actual = actual.todense()
    assert numpy.array_equal(actual, expected)


def test_iterate_parts(context):
    client = from_context(context)
    for part in client["x"].parts:
        client["x"].parts[part].read()


def test_iterate_columns(context):
    client = from_context(context)
    for col, _client in client["x"].items():
        read_from_client = _client.read()
        read_from_column = client["x"][col].read()
        read_from_full_path = client[f"x/{col}"].read()
        if col == "sps":
            read_from_client = read_from_client.todense()
            read_from_column = read_from_column.todense()
            read_from_full_path = read_from_full_path.todense()
        assert numpy.array_equal(read_from_client, read_from_column)
        assert numpy.array_equal(read_from_client, read_from_full_path)
        assert numpy.array_equal(read_from_full_path, read_from_column)


def test_metadata(context):
    client = from_context(context)
    assert client["x"].metadata == md

    # Check metadata for each part
    for part in client["x"].parts:
        c = client["x"].parts[part]
        assert c.metadata["md_key"] == f"md_for_{part}"


def test_parts_not_directly_accessible(context):
    client = from_context(context)
    client["x"].parts["df1"].read()
    client["x"].parts["df1"]["A"].read()
    client["x"]["A"].read()
    with pytest.raises(KeyError):
        client["x"]["df1"].read()
    with pytest.raises(KeyError):
        client["x/df1"].read()


def test_external_assets(context, tiff_sequence, csv_file):
    client = from_context(context)
    tiff_assets = [
        Asset(
            data_uri=f"file://localhost/{fpath}",
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
            data_uri=f"file://localhost/{csv_file}",
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

    y = client.create_composite(key="y")
    y.new(
        structure_family=StructureFamily.array,
        data_sources=[tiff_data_source],
        key="image",
    )
    y.new(
        structure_family=StructureFamily.table,
        data_sources=[csv_data_source],
        key="table",
    )

    arr = y.parts["image"].read()
    assert numpy.array_equal(arr, img_data)

    df = y.parts["table"].read()
    for col in df.columns:
        assert numpy.array_equal(df[col], df3[col])

    assert set(y.keys()) == {"image", "col1", "col2"}


def test_read_full(context_for_read):
    client = from_context(context_for_read)
    ds = client["x"].read()

    assert isinstance(ds, xarray.Dataset)
    assert set(ds.data_vars) == {
        "arr1",
        "arr2",
        "A",
        "B",
        "C",
        "D",
        "E",
        "sps",
        "img",
        "col1",
        "col2",
    }


def test_read_selective(context_for_read):
    client = from_context(context_for_read)
    ds = client["x"].read(variables=["arr1", "arr2", "A", "B", "sps"])

    assert isinstance(ds, xarray.Dataset)
    assert set(ds.data_vars) == {"arr1", "arr2", "A", "B", "sps"}


@pytest.mark.parametrize("dim0", ["time", "col1"])
def test_read_selective_with_dim0(context, dim0):
    client = from_context(context)
    ds = client["x"].read(variables=["arr2", "img", "col1"], dim0=dim0)

    assert isinstance(ds, xarray.Dataset)
    assert set(ds.data_vars) == {"arr2", "img", "col1"}.difference([dim0])

    # Check the dimension names
    for var_name in ds.data_vars:
        assert ds[var_name].dims[0] == dim0
