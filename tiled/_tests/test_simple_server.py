import platform
from pathlib import Path

import httpx
import pyarrow
import pytest

from tiled.client import from_uri
from tiled.server import SimpleTiledServer


def test_default():
    "Smoke test a server with defaults (no parameters)"
    with SimpleTiledServer() as server:
        client = from_uri(server.uri)

        # Write and read array data
        x = client.write_array([1, 2, 3], key="x")
        x[:]

        # Write and read tabular data to the SQL storage
        table = pyarrow.Table.from_pydict({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})
        y = client.create_appendable_table(table.schema, key="y")
        y.append_partition(table, 0)
        y.read()

        repr(server)
        server._repr_html_()  # impl, used by Jupyter
        # Web UI
        response = httpx.get(server.web_ui_link).raise_for_status()
        assert response.headers["content-type"].startswith("text/html")


def test_one_at_a_time():
    "We cannot run two uvicorn servers in one process."
    # Two servers start on different ports.
    MSG = "Only one server can be run at a time in a given Python process."
    with SimpleTiledServer():
        with pytest.raises(RuntimeError, match=MSG):
            SimpleTiledServer()


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
        y.append_partition(table, 0)
        assert "x" in client1
        assert "y" in client1
    with SimpleTiledServer(directory=tmp_path) as server2:
        client2 = from_uri(server2.uri)
        assert "x" in client2
        assert "y" in client2
        assert client2["x"].read() is not None
        assert client2["y"].read() is not None
    assert server1.directory == server2.directory == tmp_path


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
