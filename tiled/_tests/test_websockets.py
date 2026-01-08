import sys

import dask.array
import msgpack
import numpy as np
import pytest
from starlette.testclient import WebSocketDenialResponse

from ..client import from_context
from ..config import parse_configs

pytestmark = pytest.mark.skipif(
    sys.platform == "win32", reason="Requires Redis service"
)


def send_ws_updates(client, update_func, start=1, count=1, persist=None):
    """Helper to send updates via Tiled client in websocket tests."""
    for i in range(start, start + count):
        new_arr = np.arange(10) + i
        update_func(client, new_arr, i, persist=persist)


# An update_func for send_ws_updates
def overwrite_array(client, new_arr, seq_num, persist=None):
    _ = seq_num  # seq_num is unused for these updates
    client.write(new_arr, persist=persist)


# An update_func for send_ws_updates
def write_array_block(client, new_arr, seq_num, persist=None):
    client.write_block(new_arr, block=(seq_num - 1, 0), persist=persist)


# An update_func for send_ws_updates
def patch_array(client, new_arr, seq_num, persist=None):
    _ = seq_num  # seq_num is unused for these updates
    client.patch(new_arr, offset=(0,), persist=persist)


# An update_func for send_ws_updates
def append_array(client, new_arr, seq_num, persist=None):
    client.patch(new_arr, offset=(10 * seq_num,), extend=True, persist=persist)


def receive_ws_updates(websocket, count=1):
    """Helper to receive updates in websocket tests."""
    # Receive all updates
    received = []
    for _ in range(count + 1):  # +1 for schema
        msg_bytes = websocket.receive_bytes()
        msg = msgpack.unpackb(msg_bytes)
        received.append(msg)

    # Verify all messages received (schema + n updates)
    assert len(received) == count + 1

    return received


def verify_ws_updates(received, start=1, chunked=False):
    """Verify that we received messages with the expected data"""
    for i, msg in enumerate(received):
        if i == 0:  # schema
            assert "type" in msg
            assert "version" in msg
        else:
            assert "type" in msg
            assert "timestamp" in msg
            assert "payload" in msg
            if chunked:
                assert msg["shape"] == [1, 10]
            else:
                assert msg["shape"] == [10]

            # Verify payload contains the expected array data
            payload_array = np.frombuffer(msg["payload"], dtype=np.int64)
            expected_array = np.arange(10) + (start - 1) + i
            np.testing.assert_array_equal(payload_array, expected_array)


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
        # Send 3 updates using Tiled client that overwrite the array
        send_ws_updates(streaming_node, overwrite_array, count=3)

        # Receive and validate all updates
        received = receive_ws_updates(websocket, count=3)
        verify_ws_updates(received)


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
        # Send 2 more updates that overwrite the array
        send_ws_updates(streaming_node, overwrite_array, start=2, count=2)

        # Should only receive the 2 new updates
        received = receive_ws_updates(websocket, count=2)
        # Content starts with update #2
        verify_ws_updates(received, start=2)


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
        # Schema
        schema_msg_bytes = websocket.receive_bytes()
        schema_msg = msgpack.unpackb(schema_msg_bytes)
        assert "type" in schema_msg
        assert "version" in schema_msg

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


@pytest.mark.parametrize("write_op", (overwrite_array, patch_array))
@pytest.mark.parametrize("persist", (None, True, False))
def test_updates_persist_write(tiled_websocket_context, write_op, persist):
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
        # Send 3 updates using Tiled client that write values into the array
        send_ws_updates(streaming_node, write_op, count=3, persist=persist)

        # Receive and validate all updates
        received = receive_ws_updates(websocket, count=3)
        verify_ws_updates(received)

    # Verify values of persisted data
    if persist or persist is None:
        expected_persisted = np.arange(10) + 3  # Final sent values
    else:
        expected_persisted = arr  # Original values
    persisted_data = streaming_node.read()
    np.testing.assert_array_equal(persisted_data, expected_persisted)


@pytest.mark.parametrize("persist", (None, True, False))
def test_updates_persist_write_block(tiled_websocket_context, persist):
    context = tiled_websocket_context
    client = from_context(context)
    test_client = context.http_client

    # Create a streaming chunked array node using Tiled client
    _arr = np.array([np.arange(10) for _ in range(3)])
    arr = dask.array.from_array(_arr, chunks=(1, 10))  # Chunk along first axis
    streaming_node = client.write_array(arr, key="test_stream_immediate")

    # Connect WebSocket using TestClient with msgpack format and authorization
    with test_client.websocket_connect(
        "/api/v1/stream/single/test_stream_immediate?envelope_format=msgpack",
        headers={"Authorization": "Apikey secret"},
    ) as websocket:
        # Send 3 updates using Tiled client that write values into the array
        send_ws_updates(streaming_node, write_array_block, count=3, persist=persist)

        # Receive and validate all updates
        received = receive_ws_updates(websocket, count=3)
        verify_ws_updates(received, chunked=True)

    # Verify values of persisted data
    if persist or persist is None:
        # Combined effect of all sent values
        expected_persisted = np.array([np.arange(10) + i for i in range(1, 4)])
    else:
        # Original values
        expected_persisted = arr
    persisted_data = streaming_node.read()
    np.testing.assert_array_equal(persisted_data, expected_persisted)


# Extending an array with persist=False is not yet supported
@pytest.mark.parametrize("persist", (None, True))
def test_updates_persist_append(tiled_websocket_context, persist):
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
        # Send 3 updates using Tiled client that append to the array
        send_ws_updates(streaming_node, append_array, count=3, persist=persist)

        # Receive and validate all updates
        received = receive_ws_updates(websocket, count=3)
        verify_ws_updates(received)

    # Verify values of persisted data
    if persist or persist is None:
        # Combined effect of all sent values
        expected_persisted = np.array(
            [np.arange(10) + i for i in range(0, 4)]
        ).flatten()
    else:
        # Original values
        expected_persisted = arr
    persisted_data = streaming_node.read()
    np.testing.assert_array_equal(persisted_data, expected_persisted)


def test_updates_append_without_persist(tiled_websocket_context):
    context = tiled_websocket_context
    client = from_context(context)

    # Create streaming array node using Tiled client
    arr = np.arange(10)
    streaming_node = client.write_array(arr, key="test_stream_immediate")

    with pytest.raises(ValueError, match="Cannot PATCH an array with both parameters"):
        # Extending an array with persist=False is not yet supported
        send_ws_updates(streaming_node, append_array, count=1, persist=False)


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

    # close_stream is idempotent, so closing again should also return 200
    assert response.status_code == 200


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


def test_websocket_connection_no_api_key(tiled_websocket_context):
    """Test websocket connection with no API key fails with 401."""

    context = tiled_websocket_context
    client = from_context(context)
    test_client = context.http_client

    # Create streaming array node using correct key
    arr = np.arange(10)
    client.write_array(arr, key="test_auth_websocket")

    # Strip API key so requests below are unauthenticated.
    context.api_key = None

    # Try to connect to websocket with no API key
    with pytest.raises(WebSocketDenialResponse) as exc_info:
        with test_client.websocket_connect(
            "/api/v1/stream/single/test_auth_websocket?envelope_format=msgpack",
        ):
            pass

    assert exc_info.value.status_code == 401


def test_websocket_connection_public_no_api_key(tiled_websocket_context_public):
    """Test websocket connection to a public server with no API key works."""
    context = tiled_websocket_context_public
    client = from_context(context)
    test_client = context.http_client

    # Create streaming array node using correct key
    arr = np.arange(10)
    client.write_array(arr, key="test_auth_websocket")

    # Strip API key so requests below are unauthenticated.
    context.api_key = None

    # Try to connect to (public) websocket with no API key
    with test_client.websocket_connect(
        "/api/v1/stream/single/test_auth_websocket?envelope_format=msgpack",
    ):
        pass


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


def test_streaming_cache_config(tmp_path, redis_uri):
    "Test streaming_cache config parsing"
    config_path = tmp_path / "config.yml"
    with open(config_path, "w") as file:
        file.write(
            f"""
trees:
 - path: /
   tree: catalog
   args:
     uri: "sqlite:///:memory:"
     writable_storage:
        - "file://localhost{str(tmp_path / 'data')}"
        - "duckdb:///{tmp_path / 'data.duckdb'}"
     init_if_not_exists: true
streaming_cache:
  uri: "{redis_uri}"
  data_ttl: 50
  seq_ttl: 60
  socket_timeout: 11
  socket_connect_timeout: 12
"""
        )
    # Test that the config is parsed correctly.
    config = parse_configs(config_path)
    assert config.streaming_cache.uri == redis_uri
    assert config.streaming_cache.data_ttl == 50
    assert config.streaming_cache.seq_ttl == 60
    assert config.streaming_cache.socket_timeout == 11
    assert config.streaming_cache.socket_connect_timeout == 12
