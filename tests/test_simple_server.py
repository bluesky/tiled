import asyncio
import itertools
import json
import platform
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import httpx
import pyarrow
import pytest

from tiled.client import SERVERS, from_uri, simple
from tiled.client.register import register
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


# ----- Webhook integration tests for SimpleTiledServer


def test_webhooks_disabled_by_default():
    """Webhook endpoints must return 404 when enable_webhooks is not set."""
    with SimpleTiledServer() as server:
        resp = httpx.get(
            f"http://localhost:{server.port}/api/v1/webhooks/target/",
            headers={"Authorization": f"Apikey {server.api_key}"},
        )
        assert (
            resp.status_code == 404
        ), "Webhook router should not be mounted when enable_webhooks=False"
        assert server.webhook_secret_key is None


def test_webhooks_enabled(tmp_path):
    """enable_webhooks=True mounts the router and exposes webhook_secret_key.

    Also verifies that HTTP (non-HTTPS) localhost URLs are accepted, confirming
    the no-op validator is active instead of the production one.
    """
    with SimpleTiledServer(tmp_path, enable_webhooks=True) as server:
        assert server.webhook_secret_key is not None

        # Register a webhook pointing at a plain HTTP localhost URL —
        # this would be rejected by the production validator.
        resp = httpx.post(
            f"http://localhost:{server.port}/api/v1/webhooks/target/",
            headers={
                "Authorization": f"Apikey {server.api_key}",
                "Content-Type": "application/json",
            },
            content=json.dumps({"url": "http://localhost:19999/hook"}),
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["active"] is True
        assert "id" in body


def test_webhooks_delivers_event(tmp_path):
    """End-to-end: create a node, assert the webhook delivery arrives."""
    received = []

    # Spin up a minimal HTTP receiver on a background thread.
    class _Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            length = int(self.headers.get("Content-Length", 0))
            received.append(json.loads(self.rfile.read(length)))
            self.send_response(200)
            self.end_headers()

        def log_message(self, *args):
            pass  # suppress request logs in test output

    receiver = HTTPServer(("127.0.0.1", 0), _Handler)
    receiver_port = receiver.server_address[1]
    receiver_thread = threading.Thread(target=receiver.serve_forever, daemon=True)
    receiver_thread.start()

    try:
        with SimpleTiledServer(tmp_path, enable_webhooks=True) as server:
            client = from_uri(server.uri)

            # Register webhook pointing at our local receiver.
            resp = httpx.post(
                f"http://localhost:{server.port}/api/v1/webhooks/target/",
                headers={
                    "Authorization": f"Apikey {server.api_key}",
                    "Content-Type": "application/json",
                },
                content=json.dumps({"url": f"http://127.0.0.1:{receiver_port}/hook"}),
            )
            assert resp.status_code == 200, resp.text

            # Trigger a container-child-created event.
            client.write_array([1, 2, 3], key="x")

            # Wait for the background delivery task to complete.
            deadline = time.monotonic() + 10
            while not received and time.monotonic() < deadline:
                time.sleep(0.1)

            assert len(received) == 1
            payload = received[0]
            assert payload["type"] == "container-child-created"
            assert payload["key"] == "x"
    finally:
        receiver.shutdown()
