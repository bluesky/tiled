import asyncio
import gc

import msgpack
import numpy as np
import orjson
import pytest

from tiled.catalog import in_memory
from tiled.client import Context, from_context
from tiled.server import streaming
from tiled.server.app import build_app
from tiled.structures.bytes import BytesStructure
from tiled.structures.core import StructureFamily
from tiled.structures.data_source import Asset, DataSource, Management


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


def test_streaming_cache_config_source_validation():
    from pydantic import ValidationError

    from tiled.config import StreamingCacheConfig

    # Neither ``uri`` nor ``sentinels`` set is a configuration error, as is
    # ``sentinels`` without a ``service_name``.
    with pytest.raises(ValidationError):
        StreamingCacheConfig()
    with pytest.raises(ValidationError):
        StreamingCacheConfig(sentinels=["h1:26379"])
    # ``sentinels`` + ``service_name`` validates, leaving ``uri`` unset.
    config = StreamingCacheConfig(sentinels=["h1:26379"], service_name="mymaster")
    assert config.uri is None
    assert config.service_name == "mymaster"


def test_streaming_cache_wait_num_replicas_default():
    from tiled.config import StreamingCacheConfig

    # Standalone (uri only): the WAIT write-concern is off by default, so the
    # standalone code path is unchanged.
    standalone = StreamingCacheConfig(uri="redis://localhost:6379")
    assert standalone.wait_num_replicas == 0
    assert standalone.wait_timeout == 1000

    # Sentinel/HA cluster: WAIT auto-defaults on (1 replica).
    ha = StreamingCacheConfig(sentinels=["h1:26379"], service_name="mymaster")
    assert ha.wait_num_replicas == 1


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


def test_put_data_source_on_non_array_with_streaming_cache(tmpdir):
    """PUT /data_source on a non-array node (e.g. `bytes`) must not
    crash when the server has a `streaming_cache` configured.

    """
    payload = b"opaque-bytes-payload"
    blob = tmpdir / "blob.bin"
    blob.write_binary(payload)

    catalog = in_memory(
        writable_storage=str(tmpdir),
        cache_config={
            "uri": "memory://",
            "data_ttl": 60,
            "seq_ttl": 60,
        },
    )
    with Context.from_app(build_app(catalog)) as ctx:
        client = from_context(ctx)
        data_source = DataSource(
            mimetype="application/octet-stream",
            assets=[
                Asset(
                    data_uri=f"file://{blob}",
                    is_directory=False,
                    size=len(payload),
                    parameter="data_uris",
                    num=0,
                )
            ],
            structure_family=StructureFamily.bytes,
            structure=BytesStructure(),
            management=Management.external,
        )
        node = client.new(
            structure_family=StructureFamily.bytes,
            data_sources=[data_source],
            key="blob",
        )

        # Fetch the freshly-created data_source (with server-assigned id)
        # and echo it back through PUT /data_source. This is the exact
        # shape of call `bluesky-tiled-plugins`' validator router makes.
        response = ctx.http_client.get(
            f"/api/v1/metadata/{node.item['id']}?include_data_sources=true"
        )
        response.raise_for_status()
        [ds] = response.json()["data"]["attributes"]["data_sources"]
        put_response = ctx.http_client.put(
            f"/api/v1/data_source/{node.item['id']}",
            json={"data_source": ds},
        )
        assert put_response.status_code == 200, put_response.text
