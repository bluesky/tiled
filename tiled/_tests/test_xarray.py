import dask.array
import numpy
import pandas
import pytest
import xarray.testing
import xarray

from ..readers.xarray import DataArrayAdapter, DatasetAdapter, VariableAdapter
from ..client import from_tree
from ..trees.in_memory import Tree
from ..client import xarray as xarray_client


array = numpy.random.random((10, 10))
tree = Tree(
    {
        "variable": VariableAdapter(
            xarray.Variable(
                data=dask.array.from_array(array),
                dims=["x", "y"],
                attrs={"thing": "stuff"},
            ),
        ),
        "data_array": DataArrayAdapter(
            xarray.DataArray(
                xarray.Variable(
                    data=dask.array.from_array(array),
                    dims=["x", "y"],
                    attrs={"thing": "stuff"},
                ),
                coords={
                    "x": dask.array.arange(len(array)),
                    "y": 10 * dask.array.arange(len(array)),
                },
            ),
        ),
        "dataset": DatasetAdapter(
            xarray.Dataset(
                {
                    "image": xarray.DataArray(
                        xarray.Variable(
                            data=dask.array.from_array(array),
                            dims=["x", "y"],
                            attrs={"thing": "stuff"},
                        ),
                        coords={
                            "x": dask.array.arange(len(array)),
                            "y": 10 * dask.array.arange(len(array)),
                        },
                    ),
                    "z": xarray.DataArray(data=dask.array.ones((len(array),))),
                },
                coords={"time": numpy.arange(len(array))},
            )
        ),
        "wide": DatasetAdapter(
            xarray.Dataset(
                {
                    f"column_{i:03}": xarray.DataArray(i * numpy.ones(10))
                    for i in range(500)
                }
            )
        ),
    }
)


@pytest.mark.parametrize("key", list(tree))
def test_xarrays(key):
    client = from_tree(tree)
    expected = tree[key].read()
    actual = client[key].read()
    xarray.testing.assert_equal(actual, expected)


def test_dataset_column_access():
    client = from_tree(tree)
    expected_dataset = tree["dataset"].read()
    client_dataset = client["dataset"]
    for col in expected_dataset:
        actual = client_dataset[col]
        expected = expected_dataset[col]
        xarray.testing.assert_equal(actual, expected)


def test_dataset_coord_access():
    client = from_tree(tree)
    expected_dataset = tree["dataset"].read()
    client_dataset = client["dataset"]

    # Coordinate on data variable
    actual = client_dataset["image"].coords["x"]
    expected = expected_dataset["image"].coords["x"]
    xarray.testing.assert_equal(actual, expected)
    # Circular reference
    actual = client_dataset["image"].coords["x"].coords["x"].coords["x"]
    xarray.testing.assert_equal(actual, expected)

    # Coordinate on dataset
    actual = client_dataset.coords["time"].read()
    expected = expected_dataset.coords["time"]
    xarray.testing.assert_equal(actual, expected)
    # Circular reference
    actual = (
        client_dataset.coords["time"]
        .read()
        .coords["time"]
        .coords["time"]
        .coords["time"]
    )
    xarray.testing.assert_equal(actual, expected)


@pytest.mark.xfail(raises=NotImplementedError)
def test_nested_coords():
    # Example from
    # https://xarray.pydata.org/en/stable/user-guide/data-structures.html#creating-a-dataset
    temp = 15 + 8 * numpy.random.randn(2, 2, 3)
    precip = 10 * numpy.random.rand(2, 2, 3)
    lon = [[-99.83, -99.32], [-99.79, -99.23]]
    lat = [[42.25, 42.21], [42.63, 42.59]]

    ds = xarray.Dataset(
        {
            "temperature": (["x", "y", "time"], temp),
            "precipitation": (["x", "y", "time"], precip),
        },
        coords={
            "lon": (["x", "y"], lon),
            "lat": (["x", "y"], lat),
            "time": pandas.date_range("2014-09-06", periods=3),
            "reference_time": pandas.Timestamp("2014-09-05"),
        },
    )
    tree = Tree({"ds": DatasetAdapter(ds)})
    client = from_tree(tree)
    expected_dataset = ds
    client_dataset = client["ds"].read()
    xarray.testing.assert_equal(client_dataset, expected_dataset)


def test_url_limit_handling():
    "Check that requests and split up to stay below the URL length limit."
    expected = tree["wide"].read()
    client = from_tree(tree)
    client["wide"].read()  # Dry run to run any one-off state-initializing requests.
    # Accumulate Requests here for later inspection.
    requests = []

    async def accumulate(request):
        # httpx.AsyncClient requires event hooks to be async functions.
        requests.append(request)

    client.context.event_hooks["request"].append(accumulate)
    actual = client["wide"].read()
    xarray.testing.assert_equal(actual, expected)
    normal_request_count = len(requests)
    original = xarray_client.URL_CHARACTER_LIMIT
    try:
        # It should never be necessary to tune this for real-world use, but we
        # use this knob as a way to test its operation.
        xarray_client.URL_CHARACTER_LIMIT = 200
        # The client will need to split this across more requests in order to
        # stay within the tighter limit.
        requests.clear()  # Empty the Request cache before the next batch of requests.
        actual = client["wide"].read()
        xarray.testing.assert_equal(actual, expected)
        higher_request_count = len(requests)
        # Tighten even more.
        xarray_client.URL_CHARACTER_LIMIT = 100
        requests.clear()  # Empty the Request cache before the next batch of requests.
        actual = client["wide"].read()
        xarray.testing.assert_equal(actual, expected)
        highest_request_count = len(requests)
    finally:
        # Restore default.
        xarray_client.URL_CHARACTER_LIMIT = original
    # The goal here is to test the *trend* not the specific values because the
    # number of requests may evolve as the library changes, but the trend should
    # hold.
    assert highest_request_count > higher_request_count > normal_request_count
