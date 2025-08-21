import sys
import threading

import numpy as np
import pytest

from ..catalog import from_uri
from ..client import Context, from_context
from ..client.stream import Subscription
from ..server.app import build_app

pytestmark = pytest.mark.skipif(
    sys.platform == "win32", reason="Requires Redis service"
)


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


def test_subscribe_immediately_after_creation_websockets(tiled_websocket_context):
    context = tiled_websocket_context
    client = from_context(context)

    # Create streaming array node using Tiled client
    arr = np.arange(10)
    streaming_node = client.write_array(
        arr, key="test_stream_immediate", is_streaming=True
    )

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
