import sys

import msgpack
import numpy as np
import pytest

from ..catalog import from_uri
from ..client import Context, from_context
from ..server.app import build_app


@pytest.fixture
def tiled_websocket_context(tmpdir):
    """Fixture that provides a Tiled context with websocket support."""
    tree = from_uri(
        "sqlite:///:memory:",
        writable_storage=[
            f"file://localhost{str(tmpdir / 'data')}",
            f"duckdb:///{tmpdir / 'data.duckdb'}",
        ],
        readable_storage=None,
        init_if_not_exists=True,
        cache_settings={
            "uri": "redis://localhost:6379",
            "ttl": 60,
        },
    )

    app = build_app(
        tree,
        authentication={"single_user_api_key": "secret"},
    )

    with Context.from_app(app) as context:
        yield context


@pytest.mark.skipif(sys.platform == "win32", reason="Requires Redis service")
def test_subscribe_immediately_after_creation_websockets(tiled_websocket_context):
    context = tiled_websocket_context
    client = from_context(context)
    test_client = context.http_client

    # Create streaming array node using Tiled client
    arr = np.arange(10)
    streaming_node = client.write_array(
        arr, key="test_stream_immediate", is_streaming=True
    )

    # Connect WebSocket using TestClient with msgpack format and authorization
    with test_client.websocket_connect(
        "/api/v1/stream/single/test_stream_immediate?envelope_format=msgpack",
        headers={"Authorization": "secret"},
    ) as websocket:
        # Write updates using Tiled client
        for i in range(1, 4):
            new_arr = np.arange(10) + i
            streaming_node.write(new_arr)

        # Receive all updates
        received = []
        for _ in range(3):
            msg_bytes = websocket.receive_bytes()
            msg = msgpack.unpackb(msg_bytes)
            received.append(msg)

        # Verify all updates received in order
        assert len(received) == 3

        # Check that we received messages with the expected data
        for i, msg in enumerate(received):
            assert "timestamp" in msg
            assert "payload" in msg
            assert msg["shape"] == [10]

            # Verify payload contains the expected array data
            payload_array = np.frombuffer(msg["payload"], dtype=np.int64)
            expected_array = np.arange(10) + (i + 1)
            np.testing.assert_array_equal(payload_array, expected_array)


@pytest.mark.skipif(sys.platform == "win32", reason="Requires Redis service")
def test_websocket_connection_to_non_existent_node(tiled_websocket_context):
    """Test websocket connection to non-existent node returns 404."""
    context = tiled_websocket_context
    test_client = context.http_client

    non_existent_node_id = "definitely_non_existent_websocket_node_99999999"

    # Try to connect to websocket for non-existent node
    # This should result in an HTTP 404 response during the handshake
    response = test_client.get(
        f"/api/v1/stream/single/{non_existent_node_id}",
        headers={"Authorization": "secret"},
    )
    assert response.status_code == 404


@pytest.mark.skipif(sys.platform == "win32", reason="Requires Redis service")
def test_subscribe_after_first_update_websockets(tiled_websocket_context):
    """Client that subscribes after first update sees only subsequent updates."""
    context = tiled_websocket_context
    client = from_context(context)
    test_client = context.http_client

    # Create streaming array node using Tiled client
    arr = np.arange(10)
    streaming_node = client.write_array(
        arr, key="test_stream_after_update", is_streaming=True
    )

    # Write first update before subscribing
    first_update = np.arange(10) + 1
    streaming_node.write(first_update)

    # Connect WebSocket after first update
    with test_client.websocket_connect(
        "/api/v1/stream/single/test_stream_after_update?envelope_format=msgpack",
        headers={"Authorization": "secret"},
    ) as websocket:
        # Write more updates
        for i in range(2, 4):
            new_arr = np.arange(10) + i
            streaming_node.write(new_arr)

        # Should only receive the 2 new updates
        received = []
        for _ in range(2):
            msg_bytes = websocket.receive_bytes()
            msg = msgpack.unpackb(msg_bytes)
            received.append(msg)

        # Verify only new updates received
        assert len(received) == 2

        # Check that we received messages with the expected data
        for i, msg in enumerate(received):
            assert "timestamp" in msg
            assert "payload" in msg
            assert msg["shape"] == [10]

            # Verify payload contains the expected array data
            payload_array = np.frombuffer(msg["payload"], dtype=np.int64)
            expected_array = np.arange(10) + (
                i + 2
            )  # i+2 because we start from update 2
            np.testing.assert_array_equal(payload_array, expected_array)


@pytest.mark.skipif(sys.platform == "win32", reason="Requires Redis service")
def test_subscribe_after_first_update_from_beginning_websockets(
    tiled_websocket_context,
):
    """Client that subscribes after first update but requests from seq_num=0 sees all updates.

    Note: seq_num starts at 1 for the first data point. seq_num=0 means "start as far back
    as you have" (similar to Bluesky social)
    """
    context = tiled_websocket_context
    client = from_context(context)
    test_client = context.http_client

    # Create streaming array node using Tiled client
    arr = np.arange(10)
    streaming_node = client.write_array(
        arr, key="test_stream_from_beginning", is_streaming=True
    )

    # Write first update before subscribing
    first_update = np.arange(10) + 1
    streaming_node.write(first_update)

    # Connect WebSocket requesting from beginning
    with test_client.websocket_connect(
        "/api/v1/stream/single/test_stream_from_beginning?envelope_format=msgpack&start=0",
        headers={"Authorization": "secret"},
    ) as websocket:
        # First, should receive the initial array creation
        historical_msg_bytes = websocket.receive_bytes()
        historical_msg = msgpack.unpackb(historical_msg_bytes)
        assert "timestamp" in historical_msg
        assert "payload" in historical_msg
        assert historical_msg["shape"] == [10]

        # Verify historical payload (initial array creation - sequence 0)
        historical_payload = np.frombuffer(historical_msg["payload"], dtype=np.int64)
        expected_historical = np.arange(10)  # Initial array
        np.testing.assert_array_equal(historical_payload, expected_historical)

        # Next, should receive the first update (sequence 1)
        first_update_bytes = websocket.receive_bytes()
        first_update_msg = msgpack.unpackb(first_update_bytes)
        first_update_payload = np.frombuffer(
            first_update_msg["payload"], dtype=np.int64
        )
        expected_first_update = np.arange(10) + 1
        np.testing.assert_array_equal(first_update_payload, expected_first_update)

        # Write more updates
        for i in range(2, 4):
            new_arr = np.arange(10) + i
            streaming_node.write(new_arr)

        # Receive the new updates
        for i in range(2, 4):
            msg_bytes = websocket.receive_bytes()
            msg = msgpack.unpackb(msg_bytes)
            assert "timestamp" in msg
            assert "payload" in msg
            assert msg["shape"] == [10]

            # Verify payload contains the expected array data
            payload_array = np.frombuffer(msg["payload"], dtype=np.int64)
            expected_array = np.arange(10) + i
            np.testing.assert_array_equal(payload_array, expected_array)
