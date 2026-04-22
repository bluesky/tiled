import datetime
import sys
import urllib.parse

import dask.array
import jose.jwt
import msgpack
import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from starlette.testclient import TestClient, WebSocketDenialResponse

from tiled.catalog import from_uri
from tiled.client import from_context
from tiled.client.context import Context
from tiled.config import parse_configs

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
    envelope_format = urllib.parse.parse_qs(websocket.scope["query_string"].decode())[
        "envelope_format"
    ][0]
    received = []
    for _ in range(count + 1):  # +1 for schema
        if envelope_format == "json":
            msg = websocket.receive_json()
        else:  # default to msgpack
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
            if isinstance(msg["payload"], bytes):
                payload_array = np.frombuffer(msg["payload"], dtype=np.int64)
            else:
                payload_array = np.array(msg["payload"], dtype=np.int64)
            expected_array = (np.arange(10) + (start - 1) + i).reshape(msg["shape"])
            np.testing.assert_array_equal(
                payload_array.reshape(msg["shape"]), expected_array
            )


@pytest.mark.parametrize("envelope_format", (["msgpack", "json"]))
def test_subscribe_immediately_after_creation_websockets(
    tiled_websocket_context, envelope_format
):
    context = tiled_websocket_context
    client = from_context(context)
    test_client = context.http_client

    # Create streaming array node using Tiled client
    arr = np.arange(10)
    streaming_node = client.write_array(arr, key="test_stream_immediate")

    # Connect WebSocket using TestClient with specified envelope format and authorization
    with test_client.websocket_connect(
        f"/api/v1/stream/single/test_stream_immediate?envelope_format={envelope_format}",
        headers={"Authorization": "Apikey secret"},
    ) as websocket:
        # Send 3 updates using Tiled client that overwrite the array
        send_ws_updates(streaming_node, overwrite_array, count=3)

        # Receive and validate all updates
        received = receive_ws_updates(websocket, count=3)
        verify_ws_updates(received)


@pytest.mark.parametrize("envelope_format", (["msgpack", "json"]))
def test_connection_to_non_existent_node(tiled_websocket_context, envelope_format):
    """Test websocket connection to non-existent node returns 404."""
    context = tiled_websocket_context
    test_client = context.http_client

    non_existent_node_id = "definitely_non_existent_websocket_node_99999999"

    # Try to connect to websocket for non-existent node
    # This should result in an HTTP 404 response during the handshake
    response = test_client.get(
        f"/api/v1/stream/single/{non_existent_node_id}?envelope_format={envelope_format}",
        headers={"Authorization": "Apikey secret"},
    )
    assert response.status_code == 404


@pytest.mark.parametrize("envelope_format", (["msgpack", "json"]))
def test_subscribe_after_first_update_websockets(
    tiled_websocket_context, envelope_format
):
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
        f"/api/v1/stream/single/test_stream_after_update?envelope_format={envelope_format}",
        headers={"Authorization": "Apikey secret"},
    ) as websocket:
        # Send 2 more updates that overwrite the array
        send_ws_updates(streaming_node, overwrite_array, start=2, count=2)

        # Should only receive the 2 new updates
        received = receive_ws_updates(websocket, count=2)
        # Content starts with update #2
        verify_ws_updates(received, start=2)


@pytest.mark.parametrize("envelope_format", (["msgpack", "json"]))
def test_subscribe_after_first_update_from_beginning_websockets(
    tiled_websocket_context, envelope_format
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
        f"/api/v1/stream/single/test_stream_from_beginning?envelope_format={envelope_format}&start=0",
        headers={"Authorization": "Apikey secret"},
    ) as websocket:
        # Schema
        if envelope_format == "json":
            schema_msg = websocket.receive_json()
        else:
            schema_msg_bytes = websocket.receive_bytes()
            schema_msg = msgpack.unpackb(schema_msg_bytes)
        assert "type" in schema_msg
        assert "version" in schema_msg

        # First, should receive the initial array creation
        if envelope_format == "json":
            historical_msg = websocket.receive_json()
        else:
            historical_msg_bytes = websocket.receive_bytes()
            historical_msg = msgpack.unpackb(historical_msg_bytes)
        assert "timestamp" in historical_msg
        assert "payload" in historical_msg
        assert historical_msg["shape"] == [10]

        # Verify historical payload (initial array creation - sequence 0)
        if envelope_format == "json":
            historical_payload = np.array(historical_msg["payload"], dtype=np.int64)
        else:
            historical_payload = np.frombuffer(
                historical_msg["payload"], dtype=np.int64
            )
        expected_historical = np.arange(10)  # Initial array
        np.testing.assert_array_equal(historical_payload, expected_historical)

        # Next, should receive the first update (sequence 1)
        if envelope_format == "json":
            first_update_msg = websocket.receive_json()
            first_update_payload = np.array(first_update_msg["payload"], dtype=np.int64)
        else:
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
            if envelope_format == "json":
                msg = websocket.receive_json()
            else:
                msg_bytes = websocket.receive_bytes()
                msg = msgpack.unpackb(msg_bytes)
            assert "timestamp" in msg
            assert "payload" in msg
            assert msg["shape"] == [10]

            # Verify payload contains the expected array data
            if envelope_format == "json":
                payload_array = np.array(msg["payload"], dtype=np.int64)
            else:
                payload_array = np.frombuffer(msg["payload"], dtype=np.int64)
            expected_array = np.arange(10) + i
            np.testing.assert_array_equal(payload_array, expected_array)


@pytest.mark.parametrize("envelope_format", (["msgpack", "json"]))
@pytest.mark.parametrize("write_op", (overwrite_array, patch_array))
@pytest.mark.parametrize("persist", (None, True, False))
def test_updates_persist_write(
    tiled_websocket_context, envelope_format, write_op, persist
):
    context = tiled_websocket_context
    client = from_context(context)
    test_client = context.http_client

    # Create streaming array node using Tiled client
    arr = np.arange(10)
    streaming_node = client.write_array(arr, key="test_stream_immediate")

    # Connect WebSocket using TestClient with msgpack format and authorization
    with test_client.websocket_connect(
        f"/api/v1/stream/single/test_stream_immediate?envelope_format={envelope_format}",
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


@pytest.mark.parametrize("envelope_format", (["msgpack", "json"]))
@pytest.mark.parametrize("persist", (None, True, False))
def test_updates_persist_write_block(tiled_websocket_context, envelope_format, persist):
    context = tiled_websocket_context
    client = from_context(context)
    test_client = context.http_client

    # Create a streaming chunked array node using Tiled client
    _arr = np.array([np.arange(10) for _ in range(3)])
    arr = dask.array.from_array(_arr, chunks=(1, 10))  # Chunk along first axis
    streaming_node = client.write_array(arr, key="test_stream_immediate")

    # Connect WebSocket using TestClient with msgpack format and authorization
    with test_client.websocket_connect(
        f"/api/v1/stream/single/test_stream_immediate?envelope_format={envelope_format}",
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
@pytest.mark.parametrize("envelope_format", (["msgpack", "json"]))
@pytest.mark.parametrize("persist", (None, True))
def test_updates_persist_append(tiled_websocket_context, envelope_format, persist):
    context = tiled_websocket_context
    client = from_context(context)
    test_client = context.http_client

    # Create streaming array node using Tiled client
    arr = np.arange(10)
    streaming_node = client.write_array(arr, key="test_stream_immediate")

    # Connect WebSocket using TestClient with msgpack format and authorization
    with test_client.websocket_connect(
        f"/api/v1/stream/single/test_stream_immediate?envelope_format={envelope_format}",
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


@pytest.mark.parametrize("envelope_format", (["msgpack", "json"]))
def test_connection_wrong_api_key(tiled_websocket_context, envelope_format):
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
            f"/api/v1/stream/single/test_auth_websocket?envelope_format={envelope_format}",
            headers={"Authorization": "Apikey wrong_key"},
        ):
            pass

    assert exc_info.value.status_code == 401


@pytest.mark.parametrize("envelope_format", (["msgpack", "json"]))
@pytest.mark.parametrize(
    "auth_message,expected_close_code",
    [
        pytest.param(
            {"type": "auth", "api_key": "wrong_secret"},
            4003,
            id="wrong-api-key",
        ),
        pytest.param(
            {"type": "auth", "access_token": "invalid_token"},
            4003,
            id="invalid-access-token",
        ),
        pytest.param(
            {"type": "subscribe", "path": "foo"},
            4001,
            id="non-auth-message-type",
        ),
    ],
)
def test_first_message_auth_rejected(
    tiled_websocket_context, envelope_format, auth_message, expected_close_code
):
    """First-message auth with bad credentials or wrong message type is rejected."""
    context = tiled_websocket_context
    client = from_context(context)
    test_client = context.http_client

    arr = np.arange(10)
    client.write_array(arr, key="test_first_msg_rejected")

    context.api_key = None

    with test_client.websocket_connect(
        f"/api/v1/stream/single/test_first_msg_rejected?envelope_format={envelope_format}",
    ) as websocket:
        websocket.send_json(auth_message)
        msg = websocket.receive()
        assert msg["type"] == "websocket.close"
        assert msg.get("code") == expected_close_code


@pytest.mark.parametrize("envelope_format", (["msgpack", "json"]))
def test_first_message_auth_with_api_key(tiled_websocket_context, envelope_format):
    """Test websocket first-message authentication with a valid API key."""

    context = tiled_websocket_context
    client = from_context(context)
    test_client = context.http_client

    # Create streaming array node
    arr = np.arange(10)
    streaming_node = client.write_array(arr, key="test_first_msg_auth")

    # Connect WITHOUT API key in headers — server will accept and wait for
    # first-message auth.
    with test_client.websocket_connect(
        f"/api/v1/stream/single/test_first_msg_auth?envelope_format={envelope_format}",
    ) as websocket:
        # Send first-message auth with valid API key
        websocket.send_json({"type": "auth", "api_key": "secret"})
        # Send an update and receive it, proving auth worked
        send_ws_updates(streaming_node, overwrite_array, count=1)
        received = receive_ws_updates(websocket, count=1)
        verify_ws_updates(received)


@pytest.mark.parametrize("envelope_format", (["msgpack", "json"]))
def test_connection_public_no_api_key(tiled_websocket_context_public, envelope_format):
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
        f"/api/v1/stream/single/test_auth_websocket?envelope_format={envelope_format}",
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


@pytest.mark.parametrize("envelope_format", (["msgpack", "json"]))
def test_table_write_websocket(tiled_websocket_context, envelope_format):
    """Test that writing a full table triggers a WS event with the correct payload."""
    context = tiled_websocket_context
    client = from_context(context)
    test_client = context.http_client

    df = pd.DataFrame({"label": ["a", "b"], "value": [1.0, 2.0]})
    table_node = client.write_table(df, key="test_table_write")

    with test_client.websocket_connect(
        f"/api/v1/stream/single/test_table_write?envelope_format={envelope_format}",
        headers={"Authorization": "Apikey secret"},
    ) as websocket:
        # Overwrite with new data
        df2 = pd.DataFrame({"label": ["c", "d"], "value": [3.0, 4.0]})
        table_node.write_partition(0, df2)

        # Receive schema + 1 data message
        if envelope_format == "json":
            schema_msg = websocket.receive_json()
        else:
            schema_msg = msgpack.unpackb(websocket.receive_bytes())

        assert schema_msg["type"] == "table-schema"
        assert "version" in schema_msg
        # For JSON format, arrow_schema should have been converted to str
        if envelope_format == "json":
            assert isinstance(schema_msg["arrow_schema"], str)

        if envelope_format == "json":
            data_msg = websocket.receive_json()
        else:
            data_msg = msgpack.unpackb(websocket.receive_bytes())

        assert data_msg["type"] == "table-data"
        assert "timestamp" in data_msg
        assert data_msg["append"] is False

        if envelope_format == "json":
            # stream_json transcodes Arrow IPC to dict-of-lists
            payload = data_msg["payload"]
            assert isinstance(payload, dict)
            assert payload["label"] == ["c", "d"]
            assert payload["value"] == [3.0, 4.0]
        else:
            # msgpack keeps the raw Arrow IPC bytes
            assert isinstance(data_msg["payload"], bytes)


@pytest.mark.parametrize("envelope_format", (["msgpack", "json"]))
def test_table_append_websocket(tiled_websocket_context, envelope_format):
    """Test that appending rows to a table triggers a WS event."""
    context = tiled_websocket_context
    client = from_context(context)
    test_client = context.http_client

    schema = pa.schema([("path", pa.string()), ("label", pa.string())])
    table_node = client.create_appendable_table(schema, key="test_table_append")

    with test_client.websocket_connect(
        f"/api/v1/stream/single/test_table_append?envelope_format={envelope_format}",
        headers={"Authorization": "Apikey secret"},
    ) as websocket:
        # Append some rows (reset_index=False avoids __index_level_0__ mismatch)
        df1 = pa.table({"path": ["/a/b"], "label": ["cat"]})
        table_node.append_partition(0, df1)

        df2 = pa.table({"path": ["/c/d", "/e/f"], "label": ["dog", "bird"]})
        table_node.append_partition(0, df2)

        # Receive schema + 2 data messages
        if envelope_format == "json":
            schema_msg = websocket.receive_json()
        else:
            schema_msg = msgpack.unpackb(websocket.receive_bytes())
        assert schema_msg["type"] == "table-schema"

        for expected_tbl in [df1, df2]:
            if envelope_format == "json":
                msg = websocket.receive_json()
            else:
                msg = msgpack.unpackb(websocket.receive_bytes())

            assert msg["type"] == "table-data"
            assert msg["append"] is True

            if envelope_format == "json":
                payload = msg["payload"]
                assert payload["path"] == expected_tbl.column("path").to_pylist()
                assert payload["label"] == expected_tbl.column("label").to_pylist()


@pytest.mark.parametrize("envelope_format", (["msgpack", "json"]))
def test_table_multiple_appends_with_late_subscriber(
    tiled_websocket_context, envelope_format
):
    """Late subscriber sees only new appends, not historical ones."""
    context = tiled_websocket_context
    client = from_context(context)
    test_client = context.http_client

    schema = pa.schema([("x", pa.float64())])
    table_node = client.create_appendable_table(schema, key="test_table_late_sub")

    # Append before subscribing
    table_node.append_partition(0, pa.table({"x": [1.0, 2.0]}))

    with test_client.websocket_connect(
        f"/api/v1/stream/single/test_table_late_sub?envelope_format={envelope_format}",
        headers={"Authorization": "Apikey secret"},
    ) as websocket:
        # Append after subscribing
        table_node.append_partition(0, pa.table({"x": [3.0, 4.0]}))

        # Receive schema
        if envelope_format == "json":
            schema_msg = websocket.receive_json()
        else:
            schema_msg = msgpack.unpackb(websocket.receive_bytes())
        assert schema_msg["type"] == "table-schema"

        # Receive the one new append
        if envelope_format == "json":
            msg = websocket.receive_json()
        else:
            msg = msgpack.unpackb(websocket.receive_bytes())
        assert msg["type"] == "table-data"
        assert msg["append"] is True

        if envelope_format == "json":
            assert msg["payload"]["x"] == [3.0, 4.0]


@pytest.mark.parametrize("envelope_format", (["msgpack", "json"]))
def test_table_append_from_beginning(tiled_websocket_context, envelope_format):
    """Subscriber with start=0 replays historical table appends."""
    context = tiled_websocket_context
    client = from_context(context)
    test_client = context.http_client

    schema = pa.schema([("val", pa.int64())])
    table_node = client.create_appendable_table(schema, key="test_table_from_beginning")

    # Append before subscribing
    table_node.append_partition(0, pa.table({"val": [10, 20]}))

    with test_client.websocket_connect(
        f"/api/v1/stream/single/test_table_from_beginning?envelope_format={envelope_format}&start=0",
        headers={"Authorization": "Apikey secret"},
    ) as websocket:
        # Schema
        if envelope_format == "json":
            schema_msg = websocket.receive_json()
        else:
            schema_msg = msgpack.unpackb(websocket.receive_bytes())
        assert schema_msg["type"] == "table-schema"

        # Should receive the historical append
        if envelope_format == "json":
            msg = websocket.receive_json()
        else:
            msg = msgpack.unpackb(websocket.receive_bytes())
        assert msg["type"] == "table-data"

        if envelope_format == "json":
            assert msg["payload"]["val"] == [10, 20]

        # Now append more and receive it
        table_node.append_partition(0, pa.table({"val": [30]}))

        if envelope_format == "json":
            msg2 = websocket.receive_json()
        else:
            msg2 = msgpack.unpackb(websocket.receive_bytes())
        assert msg2["type"] == "table-data"
        if envelope_format == "json":
            assert msg2["payload"]["val"] == [30]


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
        - "file://localhost{str(tmp_path / "data")}"
        - "duckdb:///{tmp_path / "data.duckdb"}"
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


@pytest.fixture(scope="function")
def authenticated_websocket_context(tmpdir, redis_uri, enter_username_password):
    """Fixture that provides a multi-user Tiled context with JWT auth and websocket support.

    Returns (context, client, access_token) where access_token is a valid JWT for 'alice'.
    Uses build_app_from_config (like the other websocket fixtures) so that Redis
    connections are created in the correct event loop.
    """
    import subprocess
    import sys

    from tiled.server.app import build_app_from_config

    database_uri = f"sqlite:///{tmpdir / 'auth.db'}"
    subprocess.run(
        [sys.executable, "-m", "tiled", "admin", "initialize-database", database_uri],
        check=True,
        capture_output=True,
    )
    config = {
        "authentication": {
            "secret_keys": ["SECRET"],
            "providers": [
                {
                    "provider": "toy",
                    "authenticator": "tiled.authenticators:DictionaryAuthenticator",
                    "args": {
                        "users_to_passwords": {"alice": "secret1", "bob": "secret2"},
                    },
                }
            ],
        },
        "database": {
            "uri": database_uri,
        },
        "trees": [
            {
                "tree": "catalog",
                "path": "/",
                "args": {
                    "uri": "sqlite:///:memory:",
                    "writable_storage": [str(tmpdir / "data")],
                    "init_if_not_exists": True,
                },
            },
        ],
        "streaming_cache": {
            "uri": redis_uri,
            "data_ttl": 600,
            "seq_ttl": 600,
            "socket_timeout": 600,
            "socket_connect_timeout": 10,
        },
    }
    app = build_app_from_config(config)
    with Context.from_app(app) as context:
        with enter_username_password("alice", "secret1"):
            client = from_context(context)
        access_token = context.tokens["access_token"]
        yield context, client, access_token


@pytest.mark.parametrize("envelope_format", (["msgpack", "json"]))
def test_first_message_auth_with_access_token(
    authenticated_websocket_context, envelope_format
):
    """Test websocket first-message authentication with a valid JWT access token."""
    context, client, access_token = authenticated_websocket_context

    arr = np.arange(10)
    client.write_array(arr, key="test_jwt_first_msg")

    # Connect without any credentials — server accepts for first-message auth.
    unauthenticated = TestClient(context.http_client.app)
    with unauthenticated.websocket_connect(
        f"/api/v1/stream/single/test_jwt_first_msg?envelope_format={envelope_format}",
    ) as websocket:
        # Send first-message auth with valid access token
        websocket.send_json({"type": "auth", "access_token": access_token})
        # Should receive schema message (auth succeeded)
        if envelope_format == "json":
            msg = websocket.receive_json()
        else:
            msg = msgpack.unpackb(websocket.receive_bytes())
        assert msg["type"] in ("array-schema", "table-schema")


@pytest.mark.parametrize("envelope_format", (["msgpack", "json"]))
def test_query_param_access_token(authenticated_websocket_context, envelope_format):
    """Test websocket connection with JWT access token as query parameter."""
    context, client, access_token = authenticated_websocket_context

    arr = np.arange(10)
    client.write_array(arr, key="test_jwt_query_param")

    # Connect with access_token as query parameter — no first-message auth needed.
    unauthenticated = TestClient(context.http_client.app)
    token_param = urllib.parse.quote(access_token, safe="")
    try:
        with unauthenticated.websocket_connect(
            f"/api/v1/stream/single/test_jwt_query_param"
            f"?envelope_format={envelope_format}&access_token={token_param}",
        ) as websocket:
            # Should receive schema message directly (auth via query param succeeded).
            if envelope_format == "json":
                msg = websocket.receive_json()
            else:
                msg = msgpack.unpackb(websocket.receive_bytes())
            assert msg["type"] in ("array-schema", "table-schema")
    except RuntimeError as exc:
        # The multi-user authenticated_websocket_context fixture creates Redis
        # connections bound to a different event loop than the WebSocket handler's
        # buffer_live_events task. This causes a harmless RuntimeError during
        # cleanup. The schema receipt above already proves auth succeeded.
        if "attached to a different loop" not in str(exc):
            raise


@pytest.mark.parametrize("envelope_format", (["msgpack", "json"]))
def test_expired_access_token_query_param(
    authenticated_websocket_context, envelope_format
):
    """Test that an expired JWT in the query parameter is rejected with 401."""
    context, client, access_token = authenticated_websocket_context

    arr = np.arange(10)
    client.write_array(arr, key="test_expired_jwt")

    # Create an expired token using the same secret key.
    expired_token = jose.jwt.encode(
        {
            "sub": "fake",
            "type": "access",
            "exp": datetime.datetime.now(datetime.timezone.utc)
            - datetime.timedelta(seconds=60),
        },
        "SECRET",
        algorithm="HS256",
    )
    token_param = urllib.parse.quote(expired_token, safe="")
    unauthenticated = TestClient(context.http_client.app)
    with pytest.raises(WebSocketDenialResponse) as exc_info:
        with unauthenticated.websocket_connect(
            f"/api/v1/stream/single/test_expired_jwt"
            f"?envelope_format={envelope_format}&access_token={token_param}",
        ):
            pass
    assert exc_info.value.status_code == 401


@pytest.mark.parametrize("envelope_format", (["msgpack", "json"]))
@pytest.mark.parametrize(
    "auth_message",
    [
        pytest.param({"type": "auth"}, id="no-credentials"),
        pytest.param(
            {"type": "auth", "access_token": "not.a.valid.jwt"},
            id="invalid-jwt",
        ),
    ],
)
def test_first_message_jwt_auth_rejected(
    authenticated_websocket_context, envelope_format, auth_message
):
    """First-message auth with missing or invalid JWT is rejected with close code 4003."""
    context, client, _ = authenticated_websocket_context

    arr = np.arange(10)
    client.write_array(arr, key="test_jwt_rejected")

    unauthenticated = TestClient(context.http_client.app)
    with unauthenticated.websocket_connect(
        f"/api/v1/stream/single/test_jwt_rejected?envelope_format={envelope_format}",
    ) as websocket:
        websocket.send_json(auth_message)
        msg = websocket.receive()
        assert msg["type"] == "websocket.close"
        assert msg.get("code") == 4003
