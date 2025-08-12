import math
import string
import warnings

import dask.array
import numpy
import pandas.testing
import pytest
import uvicorn
import zarr
from fsspec.implementations.http import HTTPFileSystem
from httpx import ASGITransport, AsyncClient
from starlette.status import HTTP_200_OK, HTTP_401_UNAUTHORIZED, HTTP_404_NOT_FOUND

from ..adapters.array import ArrayAdapter
from ..adapters.dataframe import DataFrameAdapter
from ..adapters.mapping import MapAdapter
from ..adapters.zarr import ZARR_LIB_V2
from ..server.app import build_app
from .utils import Server

url_prefixes = ["/zarr/v2"] if ZARR_LIB_V2 else ["/zarr/v2", "/zarr/v3"]
pytestmark = pytest.mark.parametrize("prefix", url_prefixes)

rng = numpy.random.default_rng(seed=42)
array_cases = {
    "dtype_b": (numpy.arange(1, 10) % 2).astype("b"),
    "dtype_i": rng.integers(-10, 10, size=10, dtype="i"),
    "dtype_uint8": rng.integers(1, 10, size=10, dtype="uint8"),
    "dtype_uint16": rng.integers(1, 10, size=10, dtype="uint16"),
    "dtype_uint64": rng.integers(1, 10, size=10, dtype="uint64"),
    "dtype_f": rng.random(10, dtype="f"),
    "dtype_c": (rng.random(10) + rng.random(10) * 1j).astype("c8"),
    "dtype_S": numpy.array([letter * 3 for letter in string.ascii_letters], dtype="S3"),
    "dtype_U": numpy.array([letter * 3 for letter in string.ascii_letters], dtype="U3"),
    "dtype_m": numpy.array(
        ["2007-07-13", "2006-01-13", "2010-08-13"], dtype="datetime64"
    )
    - numpy.datetime64("2008-01-01"),
    "dtype_M": numpy.array(
        ["2007-07-13", "2006-01-13", "2010-08-13"], dtype="datetime64"
    ),
    "dtype_struct": numpy.array(
        [("Rex", 9, 81.0), ("Fido", 3, 27.0), ("Spot", 5, 45.0)],
        dtype=[("name", "U10"), ("age", "i4"), ("weight", "f4")],
    ),
    "random_2d": rng.random((10, 10)),
}
# TODO bitfield "t", void "v", and object "O" (which is not supported by default)
scalar_cases = {
    k: numpy.array(v[0], dtype=v.dtype)
    for k, v in array_cases.items()
    if k.startswith("dtype_")
}
for v in scalar_cases.values():
    assert v.shape == ()
array_tree = MapAdapter({k: ArrayAdapter.from_array(v) for k, v in array_cases.items()})
scalar_tree = MapAdapter(
    {k: ArrayAdapter.from_array(v) for k, v in scalar_cases.items()}
)

cube_cases = {
    "tiny_cube": rng.random((10, 10, 10)),
    "tiny_hypercube": rng.random((10, 10, 10, 10, 10)),
}
cube_tree = MapAdapter({k: ArrayAdapter.from_array(v) for k, v in cube_cases.items()})
arr_with_inf = numpy.array([0, 1, numpy.nan, -numpy.inf, numpy.inf])
inf_tree = MapAdapter(
    {
        "example": ArrayAdapter.from_array(
            arr_with_inf,
            metadata={"infinity": math.inf, "-infinity": -math.inf, "nan": numpy.nan},
        )
    },
    metadata={"infinity": math.inf, "-infinity": -math.inf, "nan": numpy.nan},
)
arr_with_zero_dim = numpy.array([]).reshape((0, 100, 1, 10))
# Suppress RuntimeWarning: divide by zero encountered in true_divide from dask.array.core.
with warnings.catch_warnings():
    zero_tree = MapAdapter(
        {
            "example": ArrayAdapter.from_array(
                dask.array.from_array(arr_with_zero_dim, chunks=arr_with_zero_dim.shape)
            )
        }
    )
df = pandas.DataFrame(
    {
        "x": rng.random(size=10, dtype="float64"),
        "y": rng.integers(10, size=10, dtype="uint"),
        "z": rng.integers(-10, 10, size=10, dtype="int64"),
    }
)
table_tree = MapAdapter(
    {
        # a dataframe divided into three partitions
        "divided": DataFrameAdapter.from_pandas(df, npartitions=3),
        # a dataframe with just one partition
        "single": DataFrameAdapter.from_pandas(df, npartitions=1),
    }
)

tree = MapAdapter(
    {
        "nested": MapAdapter({"array": array_tree, "cube": cube_tree}),
        "inf": inf_tree,
        "scalar": scalar_tree,
        "zero": zero_tree,
        "table": table_tree,
        "random_2d": array_tree["random_2d"],
    }
)


def traverse_tree(tree, parent="", result=None):
    result = result or {}
    for key, val in tree.items():
        if isinstance(val, ArrayAdapter):
            result.update({f"{parent}/{key}": "array"})
        elif isinstance(val, DataFrameAdapter):
            result.update({f"{parent}/{key}": "group"})
            for col, _ in val.items():
                result.update({f"{parent}/{key}/{col}": "array"})
        else:
            result.update({f"{parent}/{key}": "group"})
            traverse_tree(val, parent=f"{parent}/{key}", result=result)
    return result


@pytest.fixture(scope="module")
def app():
    app = build_app(tree, authentication={"single_user_api_key": "secret"})
    return app


@pytest.fixture(scope="module")
def server_url(app):
    config = uvicorn.Config(app, host="127.0.0.1", port=0, log_level="info")
    server = Server(config)
    with server.run_in_thread() as url:
        yield url


@pytest.fixture(scope="module")
def fs():
    headers = {"Authorization": "Apikey secret", "Content-Type": "application/json"}
    fs = HTTPFileSystem(
        client_kwargs={"headers": headers}, asynchronous=not ZARR_LIB_V2
    )
    return fs


@pytest.mark.parametrize("path", ["", "/", "/nested", "/table/single"])
@pytest.mark.asyncio
async def test_zarr_group_routes(prefix, path, app):
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": "Apikey secret"},
        follow_redirects=True,
    ) as client:
        response = await client.get(prefix + path)
        assert response.status_code == HTTP_200_OK

        response = await client.get(prefix + path + "/.zarray")
        assert response.status_code == HTTP_404_NOT_FOUND

        response = await client.get(prefix + path + "/.zgroup")
        assert (
            response.status_code == HTTP_200_OK
            if prefix == "/zarr/v2"
            else HTTP_404_NOT_FOUND
        )


@pytest.mark.parametrize("path", ["/nested/cube/tiny_cube", "/table/single/x"])
@pytest.mark.asyncio
async def test_zarr_array_routes(prefix, path, app):
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": "Apikey secret"},
        follow_redirects=True,
    ) as client:
        response = await client.get(prefix + path)
        assert response.status_code == HTTP_200_OK

        response = await client.get(prefix + path + "/.zgroup")
        assert response.status_code == HTTP_404_NOT_FOUND

        if prefix == "/zarr/v2":
            response = await client.get(prefix + path + "/.zarray")
            assert response.status_code == HTTP_200_OK

            ndim = len(response.json().get("shape"))
            indx = ".".join(["0"] * max(ndim, 0))
            response = await client.get(prefix + path + f"/{indx}")
            assert response.status_code == HTTP_200_OK


@pytest.mark.parametrize(
    "path",
    [
        "",
        "/",
        "/nested",
        "/table/single",
        "/nested/cube/tiny_cube",
        "/table/single/x",
    ],
)
@pytest.mark.asyncio
async def test_authentication(prefix, path, app):
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": "Apikey not-secret"},
        follow_redirects=True,
    ) as client:
        response = await client.get(prefix + path)
        assert response.status_code == HTTP_401_UNAUTHORIZED

        response = await client.get(prefix + path + "/.zarray")
        assert response.status_code == HTTP_401_UNAUTHORIZED

        response = await client.get(prefix + path + "/.zgroup")
        assert response.status_code == HTTP_401_UNAUTHORIZED


def test_zarr_integration(server_url, fs, prefix):
    url = f"{server_url}{prefix}"
    grp = zarr.open(fs.get_mapper(url), mode="r")

    assert grp.store.fs == fs
    assert set(grp.keys()) == set(tree.keys())
    assert len(set(grp.group_keys())) == 5
    assert len(set(grp.array_keys())) == 1


@pytest.mark.parametrize(
    "suffix, path",
    [
        ("", "random_2d"),
        ("", "nested/array/random_2d"),
        ("nested", "array/random_2d"),
        ("nested/array", "random_2d"),
        ("nested/array/random_2d", ""),
    ],
)
@pytest.mark.parametrize("slash", ["", "/"])
def test_zarr_groups(prefix, path, suffix, slash, server_url, fs):
    expected = array_cases["random_2d"]
    url = f"{server_url}{prefix}/{suffix}{slash}"
    arr = zarr.open(fs.get_mapper(url), mode="r")
    if path:
        arr = arr[path]
    assert numpy.array_equal(arr[...], expected)


@pytest.mark.parametrize("kind", list(array_cases.keys()))
def test_array_dtypes(kind, prefix, server_url, fs):
    expected = array_cases[kind]
    url = f"{server_url}{prefix}/nested/array"
    grp = zarr.open(fs.get_mapper(url), mode="r")
    actual = grp[kind][...]
    assert numpy.array_equal(actual, expected)


@pytest.mark.parametrize("kind", list(scalar_cases))
def test_scalar_dtypes(kind, prefix, server_url, fs):
    expected = scalar_cases[kind]
    url = f"{server_url}{prefix}/scalar"
    grp = zarr.open(fs.get_mapper(url), mode="r")
    actual = grp[kind][...]
    assert numpy.array_equal(actual, expected)


@pytest.mark.parametrize("kind", list(cube_cases))
def test_cube_cases(kind, prefix, server_url, fs):
    expected = cube_cases[kind]
    url = f"{server_url}{prefix}/nested/cube"
    grp = zarr.open(fs.get_mapper(url), mode="r")
    actual = grp[kind][...]
    assert numpy.array_equal(actual, expected)


def test_infinity(prefix, server_url, fs):
    url = f"{server_url}{prefix}/inf/example"
    actual = zarr.open(fs.get_mapper(url), mode="r")[...]
    mask = numpy.isnan(arr_with_inf)
    assert numpy.array_equal(actual[~mask], arr_with_inf[~mask])
    assert numpy.isnan(actual[mask]).all()


def test_shape_with_zero(prefix, server_url, fs):
    url = f"{server_url}{prefix}/zero/example"
    actual = zarr.open(fs.get_mapper(url), mode="r")[...]
    assert numpy.array_equal(actual, arr_with_zero_dim)


def test_dataframe_group(prefix, server_url, fs):
    url = f"{server_url}{prefix}/table"
    grp = zarr.open(fs.get_mapper(url), mode="r")
    assert set(grp.keys()) == set(table_tree.keys())

    for key in grp.keys():
        for col in grp[key].keys():
            actual = grp[key][col][...]
            expected = df[col]
            assert numpy.array_equal(actual, expected)


@pytest.mark.parametrize("key", list(table_tree.keys()))
def test_dataframe_single(key, prefix, server_url, fs):
    url = f"{server_url}{prefix}/table/{key}"
    grp = zarr.open(fs.get_mapper(url), mode="r")

    for col in df.columns:
        actual = grp[col][...]
        expected = df[col]
        assert numpy.array_equal(actual, expected)


@pytest.mark.parametrize("key", list(table_tree.keys()))
def test_dataframe_column(key, prefix, server_url, fs):
    for col in df.columns:
        url = f"{server_url}{prefix}/table/{key}/{col}"
        arr = zarr.open(fs.get_mapper(url), mode="r")
        actual = arr[...]
        expected = df[col]
        assert numpy.array_equal(actual, expected)


def test_writing_not_implemented(prefix, server_url, fs):
    url = f"{server_url}{prefix}/nested/array"

    # with pytest.raises(NotImplementedError):
    #     zarr.open(fs.get_mapper(url), mode="w")

    with pytest.raises(zarr.errors.ReadOnlyError if ZARR_LIB_V2 else ValueError):
        grp = zarr.open(fs.get_mapper(url), mode="r")
        grp["random_2d"][0, 0] = 0.0
