import dask.array
import numpy
import pytest
import xarray
import xarray.testing

from ..adapters.mapping import MapAdapter
from ..adapters.xarray import DatasetAdapter
from ..client import from_tree, record_history
from ..client import xarray as xarray_client

image = numpy.random.random((3, 5))
temp = 15 + 8 * numpy.random.randn(2, 2, 3)
precip = 10 * numpy.random.rand(2, 2, 3)
lon = [[-99.83, -99.32], [-99.79, -99.23]]
lat = [[42.25, 42.21], [42.63, 42.59]]

EXPECTED = {
    "image": xarray.Dataset(
        {
            "image": xarray.DataArray(
                xarray.Variable(
                    data=dask.array.from_array(image),
                    dims=["x", "y"],
                    attrs={"thing": "stuff"},
                ),
                coords={
                    "x": dask.array.arange(image.shape[0]) / 10,
                    "y": dask.array.arange(image.shape[1]) / 50,
                },
            ),
            "z": xarray.DataArray(data=dask.array.ones(17)),
        },
        attrs={"snow": "cold"},
    ),
    "weather": xarray.Dataset(
        {
            "temperature": (["x", "y", "time"], temp),
            "precipitation": (["x", "y", "time"], precip),
        },
        coords={
            "lon": (["x", "y"], lon),
            "lat": (["x", "y"], lat),
            "time": [1, 2, 3],
        },
    ),
    "wide": xarray.Dataset(
        {f"column_{i:03}": xarray.DataArray(i * numpy.ones(2)) for i in range(10)},
        coords={"time": numpy.arange(2)},
    ),
    "ragged": xarray.Dataset(
        {
            f"{i}": xarray.DataArray(i * numpy.ones(2 * i), dims=f"dim{i}")
            for i in range(3)
        }
    ),
}

tree = MapAdapter(
    {key: DatasetAdapter.from_dataset(ds) for key, ds in EXPECTED.items()}
)
client = from_tree(tree)


@pytest.mark.parametrize("key", list(tree))
def test_xarray_dataset(key):
    expected = EXPECTED[key]
    actual = client[key].read().load()
    xarray.testing.assert_equal(actual, expected)


@pytest.mark.parametrize("key", ["image", "weather"])
def test_dataset_column_access(key):
    expected_dataset = EXPECTED[key]
    actual_dataset = client[key].read().load()
    for col in expected_dataset:
        actual = actual_dataset[col]
        expected = expected_dataset[col]
        xarray.testing.assert_equal(actual, expected)


def test_wide_table_optimization():
    wide = client["wide"]
    with record_history() as history:
        wide.read()
    # This should be just a couple requests.
    # This upper bound is somewhat arbitrary to give wiggle room for future
    # minor changes. The point is: it's much less than one request per variable.
    assert len(history.requests) < 4


def test_wide_table_optimization_off():
    wide = client["wide"]
    with record_history() as history:
        wide.read(optimize_wide_table=False)
    assert len(history.requests) >= 10


def test_url_limit_handling():
    "Check that requests and split up to stay below the URL length limit."
    expected = EXPECTED["wide"]
    dsc = client["wide"]
    dsc.read()  # Dry run to run any one-off state-initializing requests.
    # Accumulate Requests here for later inspection.
    requests = []

    async def accumulate(request):
        # httpx.AsyncClient requires event hooks to be async functions.
        requests.append(request)

    client.context.event_hooks["request"].append(accumulate)
    actual = dsc.read()
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
        actual = dsc.read()
        xarray.testing.assert_equal(actual, expected)
        higher_request_count = len(requests)
        # Tighten even more.
        xarray_client.URL_CHARACTER_LIMIT = 100
        requests.clear()  # Empty the Request cache before the next batch of requests.
        actual = dsc.read()
        xarray.testing.assert_equal(actual, expected)
        highest_request_count = len(requests)
    finally:
        # Restore default.
        xarray_client.URL_CHARACTER_LIMIT = original
    # The goal here is to test the *trend* not the specific values because the
    # number of requests may evolve as the library changes, but the trend should
    # hold.
    assert highest_request_count > higher_request_count > normal_request_count
