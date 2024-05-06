import asyncio
import contextlib
import tempfile

import numpy
import pandas
import tifffile

from tiled.catalog import in_memory
from tiled.client import Context, from_context
from tiled.client.register import register
from tiled.client.smoke import read
from tiled.client.sync import sync
from tiled.queries import Key
from tiled.server.app import build_app


@contextlib.contextmanager
def client_factory():
    with tempfile.TemporaryDirectory() as tempdir:
        catalog = in_memory(writable_storage=tempdir)
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
    asyncio.run(register(client, tmp_path))


def populate_internal(client):
    "Populate a client with uploaded internally-managed data."
    # array
    client.write_array([1, 2, 3], key="a", metadata={"color": "red"}, specs=["alpha"])
    # table
    df = pandas.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]})
    client.write_dataframe(df, key="b", metadata={"color": "green"}, specs=["beta"])
    # nested
    container = client.create_container("c")
    container.write_array(
        [1, 2, 3], key="A", metadata={"color": "red"}, specs=["alpha"]
    )
    container.write_dataframe(df, key="B", metadata={"color": "green"}, specs=["beta"])


def test_sync_internal():
    with client_factory() as dest:
        with client_factory() as source:
            populate_internal(source)
            sync(source, dest)
            assert list(source) == list(dest)
            assert list(source["c"]) == list(dest["c"])
            read(dest)


def test_sync_external(tmp_path):
    with client_factory() as dest:
        with client_factory() as source:
            populate_external(source, tmp_path)
            sync(source, dest)
            assert list(source) == list(dest)
            assert list(source["subdir"]) == list(dest["subdir"])
            read(dest)


def test_sync_search_results():
    with client_factory() as dest:
        with client_factory() as source:
            populate_internal(source)
            results = source.search(Key("color") == "red")
            sync(results, dest)
            assert list(results) == list(dest)


def test_sync_items():
    with client_factory() as dest:
        with client_factory() as source:
            populate_internal(source)
            select_items = source.items()[:2]
            sync(select_items, dest)
            assert [key for key, _ in select_items] == list(dest)


def test_sync_dict():
    with client_factory() as dest:
        with client_factory() as source:
            populate_internal(source)
            select_dict = dict(source.items()[:2])
            sync(select_dict, dest)
            assert list(select_dict) == list(dest)
