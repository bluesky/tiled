import copy
import sys
import threading
import uuid

import numpy as np
import pytest
import tifffile
from starlette.testclient import WebSocketDenialResponse

from ..client import from_context
from ..client.stream import ArraySubscription
from ..structures.array import ArrayStructure
from ..structures.core import StructureFamily
from ..structures.data_source import Asset, DataSource, Management
from ..utils import safe_json_dump

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
        subscription.disconnect()


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


def test_subscribe_to_container(
    tiled_websocket_context,
):
    """Subscribe to updates about a Container"""
    context = tiled_websocket_context
    client = from_context(context)
    streamed_nodes = []
    child_metadata_updated_updates = []
    received_event = threading.Event()
    created_3 = threading.Event()

    def child_created_cb(sub, node):
        try:
            repr(node)
            streamed_nodes.append(node)
            if len(streamed_nodes) == 3:
                created_3.set()
        except Exception as err:
            print(repr(err))

    def child_metadata_updated_cb(sub, update):
        child_metadata_updated_updates.append(update)
        received_event.set()

    with client.subscribe().start_in_thread(1) as sub:
        sub.child_created.add_callback(child_created_cb)
        sub.child_metadata_updated.add_callback(child_metadata_updated_cb)
        uploaded_nodes = []
        for i in range(3):
            # This is exposing fragility in SQLite database connection handling.
            # Once that is resolved, remove the sleep.
            import time

            time.sleep(0.1)
            unique_key = f"{uuid.uuid4().hex[:8]}"
            uploaded_nodes.append(client.create_container(unique_key))
        assert created_3.wait(timeout=5.0), "Timeout waiting for messages"
        downloaded_nodes = list(client.values())
        for up, streamed, down in zip(uploaded_nodes, streamed_nodes, downloaded_nodes):
            pass
            # TODO Make this equality exact. It's close.
            # assert up.item == streamed.item == down.item

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


def test_subscribe_to_disconnected(
    tiled_websocket_context,
):
    """Subscribe to notification that the subscription has disconnected"""
    context = tiled_websocket_context
    client = from_context(context)
    unique_key = f"test_subscribe_to_stream_closed_{uuid.uuid4().hex[:8]}"
    x = client.create_container(unique_key)

    # Disconnect before the stream is closed.
    with x.subscribe().start_in_thread() as sub:
        event = threading.Event()

        def callback(sub):
            event.set()

        sub.disconnected.add_callback(callback)
        assert not event.is_set()
        sub.disconnect()
        assert event.wait(timeout=5.0), "Timeout waiting for messages"

    # If the writer closes the stream, the client is disconnected.
    with x.subscribe().start_in_thread() as sub:
        event = threading.Event()

        def callback(sub):
            event.set()

        sub.disconnected.add_callback(callback)
        assert not event.is_set()
        x.close_stream()
        assert event.wait(timeout=5.0), "Timeout waiting for messages"


def test_subscribe_to_array_registered(tiled_websocket_context, tmp_path):
    context = tiled_websocket_context
    client = from_context(context)
    container_sub = client.subscribe()

    updates = []
    event = threading.Event()

    def on_array_updated(sub, update):
        updates.append(update)
        event.set()

    def on_child_created(sub, node):
        array_sub = node.subscribe()
        array_sub.new_data.add_callback(on_array_updated)
        array_sub.start_in_thread(1)

    container_sub.child_created.add_callback(on_child_created)

    arr = np.random.random((3, 7, 13))
    tifffile.imwrite(tmp_path / "image1.tiff", arr[0])
    tifffile.imwrite(tmp_path / "image2.tiff", arr[1])

    # Register just the first two images.
    structure = ArrayStructure.from_array(arr[:2])
    data_source = DataSource(
        management=Management.external,
        mimetype="multipart/related;type=image/tiff",
        structure_family=StructureFamily.array,
        structure=structure,
        assets=[
            Asset(
                data_uri=f"file://{tmp_path}/image1.tiff",
                is_directory=False,
                parameter="data_uris",
                num=1,
            ),
            Asset(
                data_uri=f"file://{tmp_path}/image2.tiff",
                is_directory=False,
                parameter="data_uris",
                num=2,
            ),
        ],
    )

    with container_sub.start_in_thread(1):
        x = client.new(
            structure_family=StructureFamily.array,
            data_sources=[data_source],
            metadata={},
            specs=[],
            key="x",
        )
        actual = x.read()  # smoke test
        np.testing.assert_array_equal(actual, arr[:2])
        # Add the third image.
        tifffile.imwrite(tmp_path / "image3.tiff", arr[2])
        updated_structure = ArrayStructure.from_array(arr[:])
        updated_data_source = copy.deepcopy(x.data_sources()[0])
        updated_data_source.structure = updated_structure
        updated_data_source.assets.append(
            Asset(
                data_uri=f"file://{tmp_path}/image3.tiff",
                is_directory=False,
                parameter="data_uris",
                num=3,
            ),
        )
        x.context.http_client.put(
            x.uri.replace("/metadata/", "/data_source/", 1),
            content=safe_json_dump({"data_source": updated_data_source}),
        ).raise_for_status()
        assert event.wait(timeout=5.0), "Timeout waiting for messages"
        x.close_stream()
        client.close_stream()
        x.refresh()
        actual_updated = x.read()
        np.testing.assert_array_equal(actual_updated, arr[:])
    (update,) = updates
