from pathlib import Path

from tiled.client import from_uri
from tiled.server import SimpleTiledServer


def test_default():
    "Smoke test a server with defaults (no parameters)"
    with SimpleTiledServer() as server:
        client = from_uri(server.uri)
        # Write and read data
        x = client.write_array([1, 2, 3], key="x")
        x[:]
        repr(server)
        server._repr_html_()  # impl, used by Jupyter


def test_no_collisions():
    "Two default servers use distinct ports and storage."
    # Two servers start on different ports.
    with SimpleTiledServer() as server1, SimpleTiledServer() as server2:
        assert server1.port != server2.port

        # Data is separately stored.
        client1 = from_uri(server1.uri)
        client2 = from_uri(server2.uri)
        client1.write_array([1, 2, 3], key="x1")
        client2.write_array([1, 2, 3], key="x2")
        assert list(client1) == ["x1"]
        assert list(client2) == ["x2"]


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
        assert "x" in client1
    with SimpleTiledServer(directory=tmp_path) as server2:
        client2 = from_uri(server2.uri)
        assert "x" in client2
    assert server1.directory == server2.directory == tmp_path


def test_cleanup(tmp_path):
    # Temp dir defined by SimpleTileServer is cleaned up.
    with SimpleTiledServer() as server:
        pass
    assert not Path(server.directory).exists()

    # Directory provided by user (which happens to be temp as well,
    # because this is a test) is _not_ cleaned up.
    with SimpleTiledServer(tmp_path) as server:
        pass
    assert Path(server.directory).exists()
