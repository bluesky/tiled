import asyncio
import contextlib
import tempfile

import awkward
import h5py
import numpy
import pandas
import pytest
import sparse
import tifffile

from tiled.catalog import in_memory
from tiled.client import Context, from_context
from tiled.client.register import register
from tiled.client.smoke import read
from tiled.client.sync import copy
from tiled.client.utils import ClientError
from tiled.queries import Key
from tiled.server.app import build_app


@contextlib.contextmanager
def client_factory(readable_storage=None):
    with tempfile.TemporaryDirectory() as tempdir:
        catalog = in_memory(
            writable_storage=str(tempdir), readable_storage=readable_storage
        )
        app = build_app(catalog)
        with Context.from_app(app) as context:
            client = from_context(context)
            yield client


def populate_external(client, tmp_path):
    "Populate a client with registered externally-managed data."
    subdir = tmp_path / "subdir"
    subdir.mkdir()
    # array
    image = numpy.ones((3, 5))
    for filepath in [tmp_path / "image.tiff", subdir / "nested_image.tiff"]:
        tifffile.imwrite(filepath, image)
    # table
    for filepath in [tmp_path / "table.csv", subdir / "nested_table.csv"]:
        with open(filepath, "w") as file:
            file.write("x,y\n1,2\n3,4\n")
    # container
    for filepath in [tmp_path / "group.h5", subdir / "group.h5"]:
        with h5py.File(filepath, "w") as file:
            g = file.create_group("g")
            g["data"] = numpy.arange(3)
    # Note: Tiled does not currently happen to support any formats that it
    # identifies as 'awkward' or 'sparse'. Potentially it could, and this
    # test could be expanded to include those examples.
    asyncio.run(register(client, tmp_path))


def populate_internal(client):
    "Populate a client with uploaded internally-managed data."
    # array
    client.write_array([1, 2, 3], key="a", metadata={"color": "red"}, specs=["alpha"])
    # table
    df = pandas.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]})
    client.write_dataframe(df, key="b", metadata={"color": "green"}, specs=["beta"])
    # awkward
    client.write_awkward(
        awkward.Array([1, [2, 3]]), key="d", metadata={"color": "red"}, specs=["alpha"]
    )
    # sparse
    coo = sparse.COO(
        coords=numpy.array([[2, 5]]), data=numpy.array([1.3, 7.5]), shape=(10,)
    )
    client.write_sparse(key="e", coords=coo.coords, data=coo.data, shape=coo.shape)

    # nested
    container = client.create_container("c")
    container.write_array(
        [1, 2, 3], key="A", metadata={"color": "red"}, specs=["alpha"]
    )
    container.write_dataframe(df, key="B", metadata={"color": "green"}, specs=["beta"])
    container.write_awkward(
        awkward.Array([1, [2, 3]]), key="D", metadata={"color": "red"}, specs=["alpha"]
    )
    container.write_sparse(key="E", coords=coo.coords, data=coo.data, shape=coo.shape)


def test_copy_internal():
    with client_factory() as dest:
        with client_factory() as source:
            populate_internal(source)
            copy(source, dest)
            assert list(source) == list(dest)
            assert list(source["c"]) == list(dest["c"])
            read(dest, strict=True)


def test_copy_skip_conflict():
    with client_factory() as dest:
        with client_factory() as source:
            populate_internal(source)
            copy(source, dest)
            copy(source, dest, on_conflict="skip")
            assert list(source) == list(dest)
            assert list(source["c"]) == list(dest["c"])
            read(dest, strict=True)


def test_copy_warn_conflict():
    with client_factory() as dest:
        with client_factory() as source:
            populate_internal(source)
            copy(source, dest)
            with pytest.warns(UserWarning):
                copy(source, dest, on_conflict="warn")


def test_copy_error_conflict():
    with client_factory() as dest:
        with client_factory() as source:
            populate_internal(source)
            copy(source, dest)
            with pytest.raises(ClientError):
                copy(source, dest)


def test_copy_external(tmp_path):
    with client_factory(readable_storage=[tmp_path]) as dest:
        with client_factory() as source:
            populate_external(source, tmp_path)
            copy(source, dest)
            assert list(source) == list(dest)
            assert list(source["subdir"]) == list(dest["subdir"])
            read(dest, strict=True)


def test_copy_search_results():
    with client_factory() as dest:
        with client_factory() as source:
            populate_internal(source)
            results = source.search(Key("color") == "red")
            copy(results, dest)
            assert list(results) == list(dest)


def test_copy_items():
    with client_factory() as dest:
        with client_factory() as source:
            populate_internal(source)
            select_items = source.items()[:2]
            copy(select_items, dest)
            assert [key for key, _ in select_items] == list(dest)


def test_copy_dict():
    with client_factory() as dest:
        with client_factory() as source:
            populate_internal(source)
            select_dict = dict(source.items()[:2])
            copy(select_dict, dest)
            assert list(select_dict) == list(dest)
