import sys

import msgpack
import numpy as np
import pytest

from ..catalog import from_uri
from ..client import Context, from_context
from ..server.app import build_app

pytestmark = pytest.mark.skipif(
    sys.platform == "win32", reason="Requires Redis service"
)


@pytest.fixture
def tiled_websocket_context(tmpdir, redis_uri):
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
            "uri": redis_uri,
            "ttl": 60,
            "socket_timeout": 10.0,
            "socket_connect_timeout": 10.0,
        },
    )

    app = build_app(
        tree,
        authentication={"single_user_api_key": "secret"},
    )

    with Context.from_app(app) as context:
        yield context


def test_subscribe_immediately_after_creation_websockets(tiled_websocket_context):
    context = tiled_websocket_context
    client = from_context(context)
    test_client = context.http_client

    # Create streaming array node using Tiled client
    arr = np.arange(10)
    streaming_node = client.write_array(arr, key="test_stream_immediate")

    # Connect WebSocket using TestClient with msgpack format and authorization
    with test_client.websocket_connect(
        "/api/v1/stream/single/test_stream_immediate?envelope_format=msgpack",
        headers={"Authorization": "Apikey secret"},
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


def test_websocket_connection_to_non_existent_node(tiled_websocket_context):
    """Test websocket connection to non-existent node returns 404."""
    context = tiled_websocket_context
    test_client = context.http_client

    non_existent_node_id = "definitely_non_existent_websocket_node_99999999"

    # Try to connect to websocket for non-existent node
    # This should result in an HTTP 404 response during the handshake
    response = test_client.get(
        f"/api/v1/stream/single/{non_existent_node_id}",
        headers={"Authorization": "Apikey secret"},
    )
    assert response.status_code == 404


def test_subscribe_after_first_update_websockets(tiled_websocket_context):
    """Client that subscribes after first update sees only subsequent updates."""
    context = tiled_websocket_context
    client = from_context(context)
    test_client = context.http_client

    # Create streaming array node using Tiled client
    arr = np.arange(10)
    streaming_node = client.write_array(arr, key="test_stream_after_update")

    # Write first update before subscribing
    first_update = np.arange(10) + 1
    streaming_node.write(first_update)

    # Connect WebSocket after first update
    with test_client.websocket_connect(
        "/api/v1/stream/single/test_stream_after_update?envelope_format=msgpack",
        headers={"Authorization": "Apikey secret"},
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
    streaming_node = client.write_array(arr, key="test_stream_from_beginning")

    # Write first update before subscribing
    first_update = np.arange(10) + 1
    streaming_node.write(first_update)

    # Connect WebSocket requesting from beginning
    with test_client.websocket_connect(
        "/api/v1/stream/single/test_stream_from_beginning?envelope_format=msgpack&start=0",
        headers={"Authorization": "Apikey secret"},
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


def test_close_stream_success(tiled_websocket_context):
    """Test successful close of an existing stream."""
    context = tiled_websocket_context
    client = from_context(context)
    test_client = context.http_client

    # Create a streaming array node
    arr = np.arange(10)
    streaming_node = client.write_array(arr, key="test_close_stream")

    # Upload some data
    streaming_node.write(np.arange(10) + 1)

    # Add a small delay to ensure the stream is fully established
    import time

    time.sleep(0.5)

    # Now close the stream
    response = test_client.delete(
        "/api/v1/stream/close/test_close_stream",
        headers={"Authorization": "Apikey secret"},
    )
    assert response.status_code == 200

    # Now close the stream again
    response = test_client.delete(
        "/api/v1/stream/close/test_close_stream",
        headers={"Authorization": "Apikey secret"},
    )

    # TODO: I think the test is correct and the server should be updated.
    assert response.status_code == 404


def test_close_stream_not_found(tiled_websocket_context):
    """Test close endpoint returns 404 for non-existent node."""
    context = tiled_websocket_context
    test_client = context.http_client

    non_existent_node_id = "definitely_non_existent_node_99999999"

    response = test_client.delete(
        f"/api/v1/stream/close/{non_existent_node_id}",
        headers={"Authorization": "Apikey secret"},
    )
    assert response.status_code == 404


def test_websocket_connection_wrong_api_key(tiled_websocket_context):
    """Test websocket connection with wrong API key fails with 401."""
    from starlette.testclient import WebSocketDenialResponse

    context = tiled_websocket_context
    client = from_context(context)
    test_client = context.http_client

    # Create streaming array node using correct key
    arr = np.arange(10)
    client.write_array(arr, key="test_auth_websocket")

    # Try to connect to websocket with wrong API key
    with pytest.raises(WebSocketDenialResponse) as exc_info:
        with test_client.websocket_connect(
            "/api/v1/stream/single/test_auth_websocket?envelope_format=msgpack",
            headers={"Authorization": "Apikey wrong_key"},
        ):
            pass

    assert exc_info.value.status_code == 401


def test_close_stream_wrong_api_key(tiled_websocket_context):
    """Test close endpoint returns 403 with wrong API key."""
    context = tiled_websocket_context
    client = from_context(context)
    test_client = context.http_client

    # Create streaming array node using correct key
    arr = np.arange(10)
    client.write_array(arr, key="test_auth_close")

    # Try to close stream with wrong API key
    response = test_client.delete(
        "/api/v1/stream/close/test_auth_close",
        headers={"Authorization": "Apikey wrong_key"},
    )
    assert response.status_code == 401
