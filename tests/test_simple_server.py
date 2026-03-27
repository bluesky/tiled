import asyncio
import itertools
import platform
from pathlib import Path

import httpx
import pyarrow
import pytest

from tiled.client import SERVERS, from_uri, simple
from tiled.client.register import register
from tiled.server import SimpleTiledServer


def test_default(simple_server_factory):
    "Smoke test a server with defaults (no parameters)"
    with SimpleTiledServer() as server:
        client = from_uri(server.uri)

        # Write and read array data
        x = client.write_array([1, 2, 3], key="x")
        x[:]

        # Write and read tabular data to the SQL storage
        table = pyarrow.Table.from_pydict({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})
        y = client.create_appendable_table(table.schema, key="y")
        y.append_partition(0, table)
        y.read()

        repr(server)
        server._repr_html_()  # impl, used by Jupyter
        # Web UI
        response = httpx.get(server.web_ui_link).raise_for_status()
        assert response.headers["content-type"].startswith("text/html")


def test_specified_port():
    "Run server on a user-specified port instead of a random one."
    ARBITRARY_PORT = 38593  # I hope it is free!
    with SimpleTiledServer(port=ARBITRARY_PORT) as server:
        assert server.port == ARBITRARY_PORT


def test_specified_api_key():
    "Run server with a user-specified API key instead of a random one."
    API_KEY = "secret"
    with SimpleTiledServer(api_key=API_KEY) as server:
        assert server.api_key == API_KEY


def test_persistent_data(tmp_path):
    "Write data in a specified location. Access it across a server restart."
    with SimpleTiledServer(directory=tmp_path) as server1:
        client1 = from_uri(server1.uri)
        client1.write_array([1, 2, 3], key="x")
        table = pyarrow.Table.from_pydict({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})
        y = client1.create_appendable_table(table.schema, key="y")
        y.append_partition(0, table)
        assert "x" in client1
        assert "y" in client1
    with SimpleTiledServer(directory=tmp_path) as server2:
        client2 = from_uri(server2.uri)
        assert "x" in client2
        assert "y" in client2
        assert client2["x"].read() is not None
        assert client2["y"].read() is not None
    assert server1.directory == server2.directory == tmp_path


@pytest.mark.parametrize(
    ("as_list", "as_path"), list(itertools.product([True, False], [True, False]))
)
def test_readable_storage(tmp_path, as_list, as_path):
    "Run server with a user-specified readable storage location."
    readable_storage = [tmp_path / "readable"] if as_list else tmp_path / "readable"
    if as_path:
        readable_storage = (
            [Path(p) for p in readable_storage]
            if isinstance(readable_storage, list)
            else Path(readable_storage)
        )
    with SimpleTiledServer(
        directory=tmp_path / "default", readable_storage=readable_storage
    ) as server:
        client = from_uri(server.uri)
        (tmp_path / "readable").mkdir(parents=True, exist_ok=True)
        import h5py
        import numpy

        with h5py.File(tmp_path / "readable" / "data.h5", "w") as f:
            f["x"] = numpy.array([1, 2, 3])
        asyncio.run(register(client, tmp_path / "readable"))
        assert (client["data"]["x"].read() == [1, 2, 3]).all()


def test_cleanup(tmp_path):
    if platform.system() == "Windows":
        # Windows cannot delete the logfiles because the global Python
        # logging system still has the logfiles open for appending.
        pytest.skip("Temp data directory is not cleaned up on Windows.")
    # Temp dir defined by SimpleTileServer is cleaned up.
    with SimpleTiledServer() as server:
        pass
    assert not Path(server.directory).exists()

    # Directory provided by user (which happens to be temp as well,
    # because this is a test) is _not_ cleaned up.
    with SimpleTiledServer(tmp_path) as server:
        pass
    assert Path(server.directory).exists()


def test_simple():
    # Smoke test.
    c = simple()
    ac = c.write_array([1, 2, 3])
    ac[:]
    # Cleanup.
    SERVERS.pop().close()
