import dask.array
import numpy
import orjson
import pandas
import pytest
import xarray
import xarray.testing

from ..adapters.mapping import MapAdapter
from ..adapters.xarray import DatasetAdapter
from ..client import Context, from_context, record_history
from ..serialization.xarray import serialize_json
from ..server.app import build_app
from ..structures.core import Spec
from ..utils import APACHE_ARROW_FILE_MIME_TYPE
from .utils import URL_LIMITS

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


@pytest.fixture(scope="module")
def client():
    app = build_app(tree)
    with Context.from_app(app) as context:
        client = from_context(context)
        yield client


@pytest.mark.parametrize("key", list(tree))
def test_xarray_dataset(client, key):
    expected = EXPECTED[key]
    actual = client[key].read().load()
    xarray.testing.assert_identical(actual, expected)


def test_specs(client):
    assert client["image"].specs == [Spec("xarray_dataset")]
    assert client["image"]["image"].specs == [Spec("xarray_data_var")]
    assert client["image"]["x"].specs == [Spec("xarray_coord")]


def test_specs_mutation_bug(client):
    # https://github.com/bluesky/tiled/issues/651
    ds = pandas.DataFrame({"x": numpy.array([1, 2, 3])}).to_xarray()
    tree = MapAdapter({"data": DatasetAdapter.from_dataset(ds)})
    for _ in range(2):
        # This bug caused additional, redundant specs to be appended on each
        # iteration.
        app = build_app(tree)
        with Context.from_app(app) as context:
            client = from_context(context)
            data = client["data"]
            data.read()
            assert data.specs == [Spec("xarray_dataset")]


def test_specs_override(client):
    "The 'xarray_dataset' is appended to the end if not present."
    ds = pandas.DataFrame({"x": numpy.array([1, 2, 3])}).to_xarray()
    tree = MapAdapter(
        {
            "a": DatasetAdapter.from_dataset(ds, specs=[Spec("test")]),
            "b": DatasetAdapter.from_dataset(
                ds, specs=[Spec("xarray_dataset"), Spec("test")]
            ),
            "c": DatasetAdapter.from_dataset(
                ds, specs=[Spec("test"), Spec("xarray_dataset")]
            ),
        }
    )
    app = build_app(tree)
    with Context.from_app(app) as context:
        client = from_context(context)
    assert client["a"].specs == [Spec("test"), Spec("xarray_dataset")]
    assert client["b"].specs == [Spec("xarray_dataset"), Spec("test")]
    assert client["c"].specs == [Spec("test"), Spec("xarray_dataset")]


@pytest.mark.parametrize("key", ["image", "weather"])
def test_dataset_column_access(client, key):
    expected_dataset = EXPECTED[key]
    actual_dataset = client[key].read().load()
    for col in expected_dataset:
        actual = actual_dataset[col]
        expected = expected_dataset[col]
        xarray.testing.assert_equal(actual, expected)


def test_wide_table_optimization(client):
    wide = client["wide"]
    with record_history() as history:
        wide.read()
    # This should be just a couple requests.
    # This upper bound is somewhat arbitrary to give wiggle room for future
    # minor changes. The point is: it's much less than one request per variable.
    assert len(history.requests) < 4


def test_wide_table_optimization_off(client):
    wide = client["wide"]
    with record_history() as history:
        wide.read(optimize_wide_table=False)
    assert len(history.requests) >= 10


@pytest.mark.parametrize(
    "url_limit, expected_method",
    (
        (URL_LIMITS.HUGE, "GET"),  # URL query should fit in a GET request
        (URL_LIMITS.DEFAULT, None),  # Expected method is not specified
        (URL_LIMITS.TINY, "POST"),  # URL query is too long for a GET request
    ),
    indirect=["url_limit"],
)
def test_url_limit_bypass(client, url_limit, expected_method):
    "GET requests beyond the URL length limit should become POST requests."
    expected = EXPECTED["wide"]
    expected_requests = 2  # Once for data_vars + once for coords
    dsc = client["wide"]
    dsc.read()  # Dry run to run any one-off state-initializing requests.

    with record_history() as history:
        actual = dsc.read()
        xarray.testing.assert_equal(actual, expected)

        requests = list(request for request in history.requests)
        assert len(requests) == expected_requests

        request_methods = list(request.method for request in requests)
        if expected_method == "POST":
            assert "POST" in request_methods  # At least one POST request
        elif expected_method == "GET":
            assert "POST" not in request_methods  # No POST request


@pytest.mark.parametrize("ds_node", tree.values(), ids=tree.keys())
@pytest.mark.asyncio
async def test_serialize_json(ds_node: DatasetAdapter):
    """Verify that serialized Dataset keys are a subset
    of all coordinates and variables from the Dataset.
    Index variables are removed by serialize_json().
    """
    metadata = None  # Not used
    filter_for_access = None  # Not used
    result = await serialize_json(
        APACHE_ARROW_FILE_MIME_TYPE, ds_node, metadata, filter_for_access
    )

    result_data_keys = orjson.loads(result).keys()
    ds_coords_and_vars = set(ds_node)

    assert set(result_data_keys).issubset(ds_coords_and_vars)
