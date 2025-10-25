import sys
import threading
import uuid

import numpy as np
import pytest
from starlette.testclient import WebSocketDenialResponse

from ..client import from_context
from ..client.stream import ArraySubscription

pytestmark = pytest.mark.skipif(
    sys.platform == "win32", reason="Requires Redis service"
)


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
    subscription = streaming_node.subscribe()
    subscription.new_data.add_callback(callback)

    # Start the subscription
    with subscription.start_in_thread():
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
            assert msg.shape == (10,)

            # Verify payload contains the expected array data
            payload_array = msg.data()
            expected_array = np.arange(10) + (i + 1)
            np.testing.assert_array_equal(payload_array, expected_array)

        # Clean up the subscription
        subscription.close()


def test_websocket_connection_to_non_existent_node_subscription(
    tiled_websocket_context,
):
    """Test subscription to non-existent node raises appropriate error."""
    context = tiled_websocket_context

    non_existent_node_id = "definitely_non_existent_websocket_node_99999999"

    # Create subscription for non-existent node
    subscription = ArraySubscription(
        context=context,
        segments=[non_existent_node_id],
    )

    # Attempting to start should raise WebSocketDenialResponse
    with pytest.raises(WebSocketDenialResponse):
        subscription.start_in_thread()


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
    subscription = streaming_node.subscribe()
    # Add callback and start the subscription
    subscription.new_data.add_callback(callback)
    with subscription.start_in_thread():
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
            assert msg.shape == (10,)

            # Verify payload contains the expected array data
            payload_array = msg.data()
            expected_array = np.arange(10) + (
                i + 2
            )  # i+2 because we start from update 2
            np.testing.assert_array_equal(payload_array, expected_array)


def test_subscribe_after_first_update_from_beginning_subscription(
    tiled_websocket_context,
):
    """Client that subscribes after first update but requests from start=0 sees all updates."""
    context = tiled_websocket_context
    client = from_context(context)

    # Use unique key to avoid interference from other tests
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
    subscription = streaming_node.subscribe()
    subscription.new_data.add_callback(callback)

    # Start the subscription
    with subscription.start_in_thread(start=0):
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
        assert msg.shape == (10,)
        payload_array = msg.data()
        expected_array = np.arange(10)  # Initial array
        np.testing.assert_array_equal(payload_array, expected_array)

        # Remaining messages: updates 1, 2, 3
        for i, msg in enumerate(received[1:], 1):
            assert msg.shape == (10,)

            # Verify payload contains the expected array data
            payload_array = msg.data()
            expected_array = np.arange(10) + i
            np.testing.assert_array_equal(payload_array, expected_array)

    assert subscription.closed


def test_subscribe_to_container(
    tiled_websocket_context,
):
    """Subscribe to updates about a Container"""
    context = tiled_websocket_context
    client = from_context(context)
    child_created_nodes = []
    child_metadata_updated_updates = []
    received_event = threading.Event()
    created_3 = threading.Event()

    def child_created_cb(sub, node):
        try:
            repr(node)
            child_created_nodes.append(node)
            if len(child_created_nodes) == 3:
                created_3.set()
        except Exception as err:
            print(repr(err))

    def child_metadata_updated_cb(sub, update):
        child_metadata_updated_updates.append(update)
        received_event.set()

    with client.subscribe().start_in_thread(1) as sub:
        sub.child_created.add_callback(child_created_cb)
        sub.child_metadata_updated.add_callback(child_metadata_updated_cb)
        for i in range(3):
            # This is exposing fragility in SQLite database connection handling.
            # Once that is resolved, remove the sleep.
            import time

            time.sleep(0.1)
            unique_key = f"{uuid.uuid4().hex[:8]}"
            client.create_container(unique_key)
        assert created_3.wait(timeout=5.0), "Timeout waiting for messages"
        update_keys = [node.path_parts[-1] for node in child_created_nodes]
        assert update_keys == list(client)

        assert len(child_metadata_updated_updates) == 0
        client.values().last().update_metadata({"color": "blue"})
        assert received_event.wait(timeout=5.0), "Timeout waiting for messages"
        assert len(child_metadata_updated_updates) == 1


def test_subscribe_to_stream_closed(
    tiled_websocket_context,
):
    """Subscribe to notification that the stream has closed"""
    context = tiled_websocket_context
    client = from_context(context)
    unique_key = f"test_subscribe_to_stream_closed_{uuid.uuid4().hex[:8]}"
    x = client.create_container(unique_key)
    with x.subscribe().start_in_thread() as sub:
        event = threading.Event()

        def callback(sub):
            event.set()

        sub.stream_closed.add_callback(callback)
        assert not event.is_set()
        x.close_stream()
        assert event.wait(timeout=5.0), "Timeout waiting for messages"
