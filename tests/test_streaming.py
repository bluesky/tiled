import msgpack
import numpy as np

from tiled.client import from_context
from tiled.server import streaming


def test_register_datastore_lowercases_name():
    # Ensure registering a datastore records it under the lowercase key.
    try:

        @streaming.register_datastore("MiXeDCaSe")
        class DummyStreamingDatastore:
            ...

        assert streaming._DATASTORES["mixedcase"] is DummyStreamingDatastore
    finally:
        streaming._DATASTORES.pop("mixedcase", None)


def test_streaming_cache_requires_backend():
    # Validate guard rails around missing configuration.
    try:
        streaming.StreamingCache({})
    except ValueError as exc:
        assert "backend not specified" in str(exc)
    else:
        raise AssertionError("StreamingCache should require a backend name.")


def test_streaming_cache_unknown_backend():
    # Unknown backends should surface a helpful error.
    try:
        streaming.StreamingCache({"datastore": "does-not-exist"})
    except ValueError as exc:
        assert "Unknown backend" in str(exc)
    else:
        raise AssertionError("StreamingCache should reject unknown backends.")


def test_websocket_replay_and_live_events(tiled_websocket_context):
    context = tiled_websocket_context
    client = from_context(context)

    node_key = "stream_replay_live"
    base = np.arange(6, dtype=np.int64)
    streaming_node = client.write_array(base, key=node_key)
    streaming_node.write(base + 1)
    streaming_node.write(base + 2)

    with context.http_client.websocket_connect(
        f"/api/v1/stream/single/{node_key}?envelope_format=msgpack&start=1",
        headers={"Authorization": "Apikey secret"},
    ) as websocket:
        schema_message, *replay_messages = [
            msgpack.unpackb(websocket.receive_bytes()) for _ in range(3)
        ]
        assert all(msg["shape"] == [6] for msg in replay_messages)

        live_msg = msgpack.unpackb(websocket.receive_bytes())
        assert live_msg["shape"] == [6]

        payload_array = np.frombuffer(live_msg["payload"], dtype=np.int64)
        expected = base + 2
        np.testing.assert_array_equal(payload_array, expected)

    context.http_client.delete(
        f"/api/v1/stream/close/{node_key}",
        headers={"Authorization": "Apikey secret"},
    )
