import numpy as np
import pandas as pd
import pytest
import sparse

from tiled.client.metadata_update import DELETE_KEY

from ..catalog import in_memory
from ..client import Context, from_context
from ..client.xarray import DatasetClient
from ..server.app import build_app

rng = np.random.default_rng(12345)

# 1D Arrays
time_1x = np.linspace(0, 1, 10)
time_2x = np.linspace(0, 1, 20)
arr1 = rng.random(size=(10,), dtype="float64")
arr2 = rng.random(size=(10, 1), dtype="single")
arr3 = rng.random(size=(20, 1), dtype="double")
arr4 = rng.integers(0, 255, size=(10,), dtype="uint8")

# nD Arrays
img1 = rng.random(size=(10, 13, 17), dtype="float64")
img2 = rng.random(size=(20, 13, 17), dtype="float64")

# Tables
tab1 = pd.DataFrame(
    {
        "colA": rng.random(10, dtype="float64"),
        "colB": rng.integers(0, 255, size=(10,), dtype="uint8"),
        "colC": np.random.choice(["a", "b", "c", "d", "e"], 10),
    }
)
tab2 = pd.DataFrame(
    {
        "colD": rng.random(20, dtype="float64"),
        "colE": rng.integers(0, 255, size=(20,), dtype="uint8"),
        "colF": np.random.choice(["a", "b", "c", "d", "e"], 20),
    }
)
tab3 = pd.DataFrame(
    {
        "colG": rng.random(20, dtype="float64"),
        "colH": rng.integers(0, 255, size=(20,), dtype="uint8"),
        "colI": np.random.choice(["a", "b", "c", "d", "e"], 20),
    }
)

# Sparse Arrays
sps1 = rng.random(size=(10, 13, 17), dtype="float64")
sps1 = sparse.COO(np.where(sps1 > 0.95, sps1, 0))
sps2 = sparse.COO(np.array([0, 0, 0, 1, 0, 0, 0, 0, 0, 1]))
sps3 = sparse.COO(np.array([0, 0, 0, 1, 0, 0, 0, 0, 0, 1] * 2))

data = [
    ("time", time_1x),
    ("time_1x", time_1x),
    ("time_2x", time_2x),
    ("arr1", arr1),
    ("arr2", arr2),
    ("arr3", arr3),
    ("arr4", arr4),
    ("img1", img1),
    ("img2", img2),
    ("colA", tab1["colA"]),
    ("colB", tab1["colB"]),
    ("colC", tab1["colC"]),
    ("colD", tab2["colD"]),
    ("colE", tab2["colE"]),
    ("colF", tab2["colF"]),
    # ("colG", tab3["colG"]),
    # ("colH", tab3["colH"]),
    # ("colI", tab3["colI"]),
    # ("sps1", sps1.todense()),
    # ("sps2", sps2.todense()),
    # ("sps3", sps3.todense()),
]

md = {"md_key1": "md_val1", "md_key2": 2}


@pytest.fixture(scope="module")
def tree(tmp_path_factory):
    return in_memory(writable_storage=tmp_path_factory.getbasetemp())


@pytest.fixture(scope="module")
def context(tree):
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)
        x = client.create_composite(key="x", metadata=md)
        x.write_array(time_1x, key="time", metadata={})
        x.write_array(
            time_1x,
            key="time_1x",
            metadata={},
            specs=["xarray_coord"],
            dims=["time_1x"],
        )
        x.write_array(
            time_2x,
            key="time_2x",
            metadata={},
            specs=["xarray_coord"],
            dims=["time_2x"],
        )
        x.write_array(arr1, key="arr1", metadata={})
        x.write_array(arr2, key="arr2", metadata={})
        x.write_array(arr3, key="arr3", metadata={})
        x.write_array(arr4, key="arr4", metadata={})
        x.write_array(img1, key="img1", metadata={})
        x.write_array(img2, key="img2", metadata={})

        x.write_dataframe(
            tab1,
            key="tab1",
            metadata={},
        )
        x.write_dataframe(
            tab2,
            key="tab2",
            metadata={},
        )
        # table = pyarrow.Table.from_pandas(tab3)
        # x.create_appendable_table(schema=table.schema, key="tab3")
        # x.parts["tab3"].append_partition(table, 0)

        # x.write_sparse(
        #     coords=sps1.coords,
        #     data=sps1.data,
        #     shape=sps1.shape,
        #     key="sps1",
        #     metadata={},
        # )
        # x.write_sparse(
        #     coords=sps2.coords,
        #     data=sps2.data,
        #     shape=sps2.shape,
        #     key="sps2",
        #     metadata={},
        # )
        # x.write_sparse(
        #     coords=sps3.coords,
        #     data=sps3.data,
        #     shape=sps3.shape,
        #     key="sps3",
        #     metadata={},
        # )

        yield context


def test_create_full_dataset(context):
    x = from_context(context)["x"]
    ds = x.to_dataset()
    assert isinstance(ds, DatasetClient)
    assert len(ds) == len(data)
    assert set(ds.keys()) == set([name for name, _ in data])


def test_create_partial_dataset(context):
    x = from_context(context)["x"]
    keys = ["time_1x", "arr1", "img1", "colA", "colD"]
    ds = x.to_dataset(*keys)
    assert len(ds) == len(keys)
    assert set(ds.keys()) == set(keys)


@pytest.mark.parametrize("name, expected", data)
def test_read_from_dataset(context, name, expected):
    x = from_context(context)["x"]
    ds = x.to_dataset()
    actual = ds[name].read()
    assert np.array_equal(actual, expected.squeeze())


def test_read_xarray_same_shape(context):
    x = from_context(context)["x"]

    # Use all arrays to construct the dataset; read all
    keys = ["arr1", "arr2", "arr4", "colA", "colB", "colC"]
    xarr = x.to_dataset(*keys).read()
    assert len(xarr) == len(keys)
    assert len(xarr.coords) == 0
    assert set(xarr.data_vars) == set(keys)
    assert set(xarr.variables) == set(keys)

    # Use 'time' arrays as the default coordinate
    keys = ["time", "arr1", "arr2", "arr4", "colA", "colB", "colC"]
    xarr = x.to_dataset(*keys).read()
    assert len(xarr) == len(keys) - 1
    assert set(xarr.coords) == {"time"}
    assert set(xarr.data_vars) == set(keys) - {"time"}
    assert set(xarr.variables) == set(keys)

    # Use all arrays to construct the dataset; read a subset
    keys = ["time", "arr1", "arr2", "arr4", "colA", "colB", "colC"]
    xarr = x.to_dataset(*keys).read(variables=["arr1", "colA"])
    assert len(xarr) == 2
    assert len(xarr.coords) == 0
    assert set(xarr.variables) == {"arr1", "colA"}

    # Set 'time_1x' as the default coordinate for some of the arrays
    keys = ["time", "time_1x", "arr1", "arr4", "colA", "colB"]
    x.parts["tab1"].update_metadata(
        metadata={"column_specs": {"colA": ["xarray_data_var"]}, "rows_dim": "time_1x"}
    )
    xarr = x.to_dataset(*keys).read()
    assert set(xarr.dims) == {"time", "time_1x"}
    assert set(xarr.coords) == {"time", "time_1x"}
    assert set(xarr.data_vars) == {"arr1", "arr4", "colA", "colB"}
    assert set(xarr.variables) == {"time", "time_1x", "arr1", "arr4", "colA", "colB"}

    # Revert the metadata changes
    x.parts["tab1"].update_metadata(
        metadata={"column_specs": DELETE_KEY, "rows_dim": DELETE_KEY}
    )


def test_read_xarray_with_ndarrays(context):
    x = from_context(context)["x"]

    keys = ["time", "arr1", "arr2", "arr4", "img1"]
    xarr = x.to_dataset(*keys).read()
    assert set(xarr.coords) == {"time"}
    assert set(xarr.data_vars) == {"arr1", "arr2", "arr4", "img1"}
    assert xarr["arr1"].dims == ("time",)
    assert xarr["img1"].dims == ("time", "dim1", "dim2")


def test_read_xarray_different_lengths(context):
    x = from_context(context)["x"]

    keys = ["time_1x", "time_2x", "arr1", "arr3", "img1", "img2", "colA", "colD"]

    # Set dimension labels for the arrays
    x["arr1"].update_metadata(metadata={"dims": ["time_1x"]})
    x["arr3"].update_metadata(metadata={"dims": ["time_2x"]})
    x["img1"].update_metadata(metadata={"dims": ["time_1x", "x", "y"]})
    x["img2"].update_metadata(metadata={"dims": ["time_2x", "x", "y"]})
    x.parts["tab1"].update_metadata(metadata={"rows_dim": "time_1x"})
    x.parts["tab2"].update_metadata(metadata={"rows_dim": "time_2x"})

    xarr = x.to_dataset(*keys).read()
    assert set(xarr.coords) == {"time_1x", "time_2x"}
    assert set(xarr.dims) == {"time_1x", "time_2x", "x", "y"}
    assert set(xarr.data_vars) == {"arr1", "arr3", "img1", "img2", "colA", "colD"}

    # Revert the metadata changes
    x["arr1"].update_metadata(metadata={"dims": DELETE_KEY})
    x["arr3"].update_metadata(metadata={"dims": DELETE_KEY})
    x["img1"].update_metadata(metadata={"dims": DELETE_KEY})
    x["img2"].update_metadata(metadata={"dims": DELETE_KEY})
    x.parts["tab1"].update_metadata(metadata={"rows_dim": DELETE_KEY})
    x.parts["tab2"].update_metadata(metadata={"rows_dim": DELETE_KEY})


@pytest.mark.parametrize("align", ["zip_shortest", "resample"])
def test_read_xarray_with_alignment(context, align):
    x = from_context(context)["x"]

    keys = ["time", "arr1", "arr3", "img1", "img2", "colA", "colD"]
    xarr = x.to_dataset(*keys, align=align).read()
    assert set(xarr.coords) == {"time"}
    assert set(xarr.dims) == {"time", "dim1", "dim2"}
    assert set(xarr.data_vars) == set(keys) - {"time"}
    for key in keys:
        assert xarr[key].shape[0] == 10
