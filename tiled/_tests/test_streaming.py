import asyncio
import gc

import msgpack
import numpy as np
import orjson
import pytest

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


@pytest.mark.asyncio
async def test_in_memory_cache_datastore_sequence_and_set_get():
    datastore = streaming.TTLCacheDatastore(
        {"maxsize": 10, "seq_ttl": 60, "data_ttl": 60}
    )
    node_id = "node-1"
    seq1 = await datastore.incr_seq(node_id)
    seq2 = await datastore.incr_seq(node_id)
    assert (seq1, seq2) == (1, 2)
    assert datastore._seq_cache[node_id] == 2

    metadata = {"type": "array", "shape": [2], "timestamp": "now"}
    payload = b"payload-bytes"
    await datastore.set(node_id, seq2, metadata, payload=payload)
    payload_bytes, metadata_bytes = await datastore.get(
        f"data:{node_id}:{seq2}", "payload", "metadata"
    )
    assert payload_bytes == payload
    assert orjson.loads(metadata_bytes) == metadata


@pytest.mark.asyncio
async def test_in_memory_cache_datastore_close_sets_end_of_stream():
    datastore = streaming.TTLCacheDatastore(
        {"maxsize": 10, "seq_ttl": 60, "data_ttl": 60}
    )
    node_id = "node-2"
    await datastore.close(node_id)
    payload_bytes, metadata_bytes = await datastore.get(
        "data:node-2:1", "payload", "metadata"
    )
    assert payload_bytes is None
    assert orjson.loads(metadata_bytes)["end_of_stream"] is True


@pytest.mark.asyncio
async def test_pubsub_fanout_and_cleanup():
    pubsub = streaming.PubSub()
    gen1 = pubsub.subscribe("topic")
    gen2 = pubsub.subscribe("topic")

    task1 = asyncio.create_task(gen1.__anext__())
    task2 = asyncio.create_task(gen2.__anext__())
    await pubsub.publish("topic", "hello")

    assert await asyncio.wait_for(task1, timeout=1) == "hello"
    assert await asyncio.wait_for(task2, timeout=1) == "hello"

    del task1, task2
    await gen1.aclose()
    await gen2.aclose()
    del gen1, gen2
    for _ in range(5):
        gc.collect()
        if "topic" not in pubsub._topics:
            break
        await asyncio.sleep(0)
    assert "topic" not in pubsub._topics
