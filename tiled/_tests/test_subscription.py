import sys
import threading

import numpy as np
import pytest
from starlette.testclient import WebSocketDenialResponse

from ..catalog import from_uri
from ..client import Context, from_context
from ..client.stream import Subscription
from ..server.app import build_app

pytestmark = pytest.mark.skipif(
    sys.platform == "win32", reason="Requires Redis service"
)


@pytest.fixture(scope="function")
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

    # Create streaming array node using Tiled client
    arr = np.arange(10)
    streaming_node = client.write_array(arr, key="test_stream_immediate")

    # Set up subscription using the Subscription class
    received = []
    received_event = threading.Event()

    def callback(subscription, data):
        """Callback to collect received messages."""
        received.append(data)
        if len(received) >= 3:
            received_event.set()

    # Create subscription for the streaming node
    subscription = Subscription(
        context=context,
        segments=["test_stream_immediate"],
    )
    subscription.add_callback(callback)

    # Start the subscription
    subscription.start()

    # Write updates using Tiled client
    for i in range(1, 4):
        new_arr = np.arange(10) + i
        streaming_node.write(new_arr)

    # Wait for all messages to be received
    assert received_event.wait(timeout=5.0), "Timeout waiting for messages"

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

    # Clean up the subscription
    subscription.stop()


def test_websocket_connection_to_non_existent_node_subscription(
    tiled_websocket_context,
):
    """Test subscription to non-existent node raises appropriate error."""
    context = tiled_websocket_context

    non_existent_node_id = "definitely_non_existent_websocket_node_99999999"

    # Create subscription for non-existent node
    subscription = Subscription(
        context=context,
        segments=[non_existent_node_id],
    )

    # Attempting to start should raise WebSocketDenialResponse
    with pytest.raises(WebSocketDenialResponse):
        subscription.start()


def test_subscribe_after_first_update_subscription(tiled_websocket_context):
    """Client that subscribes after first update sees only subsequent updates."""
    context = tiled_websocket_context
    client = from_context(context)

    # Create streaming array node using Tiled client
    arr = np.arange(10)
    streaming_node = client.write_array(arr, key="test_stream_after_update")

    # Write first update before subscribing
    first_update = np.arange(10) + 1
    streaming_node.write(first_update)

    # Set up subscription using the Subscription class (no start parameter = only new updates)
    received = []
    received_event = threading.Event()

    def callback(subscription, data):
        """Callback to collect received messages."""
        received.append(data)
        if len(received) >= 2:
            received_event.set()

    # Create subscription for the streaming node
    subscription = Subscription(
        context=context,
        segments=["test_stream_after_update"],
    )
    subscription.add_callback(callback)

    # Start the subscription
    subscription.start()

    # Write more updates
    for i in range(2, 4):
        new_arr = np.arange(10) + i
        streaming_node.write(new_arr)

    # Wait for messages to be received
    assert received_event.wait(timeout=5.0), "Timeout waiting for messages"

    # Should only receive the 2 new updates (not the first one)
    assert len(received) == 2

    # Check that we received messages with the expected data
    for i, msg in enumerate(received):
        assert "timestamp" in msg
        assert "payload" in msg
        assert msg["shape"] == [10]

        # Verify payload contains the expected array data
        payload_array = np.frombuffer(msg["payload"], dtype=np.int64)
        expected_array = np.arange(10) + (i + 2)  # i+2 because we start from update 2
        np.testing.assert_array_equal(payload_array, expected_array)

    # Clean up the subscription
    subscription.stop()


def test_subscribe_after_first_update_from_beginning_subscription(
    tiled_websocket_context,
):
    """Client that subscribes after first update but requests from start=0 sees all updates."""
    context = tiled_websocket_context
    client = from_context(context)

    # Use unique key to avoid interference from other tests
    import uuid

    unique_key = f"test_stream_from_beginning_{uuid.uuid4().hex[:8]}"

    # Create streaming array node using Tiled client
    arr = np.arange(10)
    streaming_node = client.write_array(arr, key=unique_key)

    # Write first update before subscribing
    first_update = np.arange(10) + 1
    streaming_node.write(first_update)

    # Set up subscription using the Subscription class with start=0
    received = []
    received_event = threading.Event()

    def callback(subscription, data):
        """Callback to collect received messages."""
        received.append(data)
        if len(received) >= 4:  # initial + first update + 2 new updates
            received_event.set()

    # Create subscription for the streaming node with start=0
    subscription = Subscription(context=context, segments=[unique_key], start=0)
    subscription.add_callback(callback)

    # Start the subscription
    subscription.start()

    # Write more updates
    for i in range(2, 4):
        new_arr = np.arange(10) + i
        streaming_node.write(new_arr)

    # Wait for all messages to be received
    assert received_event.wait(timeout=5.0), "Timeout waiting for messages"

    # Should receive: initial array + first update + 2 new updates = 4 total
    assert len(received) == 4

    # Check the messages in order
    # First message: initial array creation
    msg = received[0]
    assert "timestamp" in msg
    assert "payload" in msg
    assert msg["shape"] == [10]
    payload_array = np.frombuffer(msg["payload"], dtype=np.int64)
    expected_array = np.arange(10)  # Initial array
    np.testing.assert_array_equal(payload_array, expected_array)

    # Remaining messages: updates 1, 2, 3
    for i, msg in enumerate(received[1:], 1):
        assert "timestamp" in msg
        assert "payload" in msg
        assert msg["shape"] == [10]

        # Verify payload contains the expected array data
        payload_array = np.frombuffer(msg["payload"], dtype=np.int64)
        expected_array = np.arange(10) + i
        np.testing.assert_array_equal(payload_array, expected_array)

    # Clean up the subscription
    subscription.stop()
