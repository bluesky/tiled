import asyncio
import itertools
import logging
from collections import defaultdict
from datetime import datetime
from typing import Any, Callable, Dict, Optional, Protocol

import cachetools
import orjson
from fastapi import WebSocketDisconnect
from redis import asyncio as redis

from ..ndslice import NDSlice
from ..utils import safe_json_dump as _safe_json_dump

logger = logging.getLogger(__name__)


def safe_json_dump(content):
    """Additional handling for NDSlice serialization."""
    return _safe_json_dump(content, auto_json_types=(NDSlice,))


class StreamingDatastore(Protocol):
    def __init__(self, settings: Dict[str, Any]) -> None:
        ...

    def client(self) -> Any:
        ...

    def make_ws_handler(
        self,
        websocket,
        formatter,
        uri,
        node_id,
        schema,
    ) -> Callable[[int], None]:
        ...

    async def incr_seq(self, node_id: str) -> int:
        ...

    async def set(self, node_id, sequence, metadata, payload=None) -> None:
        ...

    async def get(self, key, *fields) -> None:
        ...

    async def close(self, node_id) -> None:
        ...


_DATASTORES: Dict[str, type[StreamingDatastore]] = {}


def register_datastore(name: str):
    """Decorator to register a backend class"""

    def _wrap(cls: type[StreamingDatastore]):
        _DATASTORES[name.lower()] = cls
        return cls

    return _wrap


class StreamingCache:
    def __init__(self, cache_settings: dict[str, Any]) -> None:
        self._settings = cache_settings
        datastore_name = (self._settings.get("datastore", "")).lower()
        if not datastore_name:
            raise ValueError("backend not specified in streaming_cache_config")
        try:
            datastore_cls = _DATASTORES[datastore_name]
        except KeyError:
            raise ValueError(
                f"Unknown backend '{datastore_name}'. Available backends: {sorted(_DATASTORES)}"
            )
        self._datastore = datastore_cls(self._settings)

    async def incr_seq(self, node_id: str) -> int:
        return await self._datastore.incr_seq(node_id)

    async def set(self, node_id, sequence, metadata, payload=None):
        await self._datastore.set(node_id, sequence, metadata, payload)

    @property
    def client(self):
        return self._datastore.client

    async def close(self, node_id):
        await self._datastore.close(node_id)

    def make_ws_handler(self, websocket, formatter, uri, node_id, schema):
        return self._datastore.make_ws_handler(
            websocket, formatter, uri, node_id, schema
        )


class PubSub:
    def __init__(self):
        self._topics = defaultdict(set)

    async def publish(self, topic: str, message):
        for q in list(self._topics.get(topic, ())):
            q.put_nowait(message)

    def subscribe(self, topic: str):
        q = asyncio.Queue()
        self._topics[topic].add(q)

        async def gen():
            try:
                while True:
                    yield await q.get()
            finally:
                self._topics[topic].discard(q)

        return gen()


def _make_ws_handler_common(
    *,
    websocket,
    formatter,
    uri: str,
    node_id: str,
    schema,
    get_func: Callable[..., Any],
    current_sequence_getter: Callable[[], Any],
    live_sequence_source: Callable[
        [], Any
    ],  # returns (AsyncIterator[int], Optional[Callable[[], Awaitable[None]]])
):
    async def handler(sequence: Optional[int] = None):
        await websocket.accept()
        end_stream = asyncio.Event()

        # Send schema to provide client context to interpret what follows.
        await formatter(websocket, schema, None)

        async def stream_data(sequence):
            """Helper function to stream a specific sequence number to a websocket"""

            key = f"data:{node_id}:{sequence}"
            payload_bytes, metadata_bytes = await get_func(key, "payload", "metadata")
            if metadata_bytes is None:
                # This means that ttl has expired for this sequence
                return
            metadata = orjson.loads(metadata_bytes)
            if metadata.get("end_of_stream"):
                # This means that the stream is closed by the producer
                end_stream.set()
                return
            if metadata.get("type") == "array-ref":
                if metadata.get("patch"):
                    s = ",".join(
                        f"{offset}:{offset+shape}"
                        for offset, shape in zip(
                            metadata["patch"]["offset"], metadata["patch"]["shape"]
                        )
                    )
                    metadata["uri"] = f"{uri}?slice={s}"
                else:
                    s = ",".join(f":{dim}" for dim in metadata["shape"])
                    metadata["uri"] = f"{uri}?slice={s}"
            await formatter(websocket, metadata, payload_bytes)

        # Setup buffer
        stream_buffer = asyncio.Queue()

        async def buffer_live_events():
            """Function that adds currently streaming data to an asyncio.Queue"""
            live_cleanup = None
            try:
                live_iter, live_cleanup = await live_sequence_source()
                async for live_seq in live_iter:
                    await stream_buffer.put(live_seq)
            except asyncio.CancelledError:
                # Task cancelled during shutdown.
                pass
            except Exception as e:
                logger.exception(
                    f"Live subscription error for node {node_id}: {e}",
                )
            finally:
                if live_cleanup is not None:
                    await live_cleanup()

        live_task = asyncio.create_task(buffer_live_events())

        if sequence is not None:
            # If a sequence number is passed, replay old data
            current_seq = int(await current_sequence_getter())
            logger.debug("Replaying old data...")
            for s in range(sequence, current_seq + 1):
                await stream_data(s)
        # Finally stream all buffered data into the websocket
        try:
            while not end_stream.is_set():
                live_seq = await stream_buffer.get()
                await stream_data(live_seq)
            else:
                await websocket.close(code=1000, reason="Producer ended stream")
        except WebSocketDisconnect:
            logger.info(f"Client disconnected from node {node_id}")
        finally:
            live_task.cancel()
            await live_task

    return handler


@register_datastore("ttlcache")
class TTLCacheDatastore(StreamingDatastore):
    def __init__(self, settings: Dict[str, Any]):
        self._settings = settings
        maxsize = self._settings.get("maxsize", 1000)
        seq_ttl = self._settings["seq_ttl"]
        self._seq_cache = cachetools.TTLCache(
            maxsize=maxsize,
            ttl=seq_ttl,
        )
        self._seq_counters = cachetools.TTLCache(
            maxsize=maxsize,
            ttl=seq_ttl,
        )
        self._data_cache = cachetools.TTLCache(
            maxsize=maxsize, ttl=self._settings["data_ttl"]
        )
        self._pubsub = PubSub()

    @property
    def client(self):
        return self

    async def incr_seq(self, node_id: str) -> int:
        counter = self._seq_counters.get(node_id)
        if counter is None:
            counter = itertools.count(1)
        sequence = next(counter)
        # Refresh TTL on each access to mimic redis' expire behavior.
        self._seq_counters[node_id] = counter
        self._seq_cache[node_id] = sequence
        return sequence

    async def set(self, node_id, sequence, metadata, payload=None):
        mapping = {
            "sequence": sequence,
            "metadata": safe_json_dump(metadata),
        }
        if payload:
            mapping["payload"] = payload
        self._data_cache[f"data:{node_id}:{sequence}"] = mapping
        await self._pubsub.publish(f"notify:{node_id}", sequence)

    async def close(self, node_id: str):
        # Increment the counter for this node.
        sequence = await self.incr_seq(node_id)
        # Publish a special message (end_of_stream) that will signal
        # any open clients to close.
        metadata = {
            "timestamp": datetime.now().isoformat(),
            "end_of_stream": True,
        }
        mapping = {"sequence": sequence, "metadata": safe_json_dump(metadata)}
        self._data_cache[f"data:{node_id}:{sequence}"] = mapping
        await self._pubsub.publish(f"notify:{node_id}", sequence)

    def make_ws_handler(self, websocket, formatter, uri, node_id, schema):
        async def current_sequence_getter():
            return int(self._seq_cache.get(node_id, 0))

        async def live_sequence_source():
            agen = self._pubsub.subscribe(f"notify:{node_id}")
            return agen, agen.aclose

        return _make_ws_handler_common(
            websocket=websocket,
            formatter=formatter,
            uri=uri,
            node_id=node_id,
            schema=schema,
            get_func=self.get,
            current_sequence_getter=current_sequence_getter,
            live_sequence_source=live_sequence_source,
        )


@register_datastore("redis")
class RedisStreamingDatastore(StreamingDatastore):
    def __init__(self, settings: Dict[str, Any]):
        self._settings = settings
        socket_timeout = self._settings["socket_timeout"]
        socket_connect_timeout = self._settings["socket_connect_timeout"]
        self._client = redis.from_url(
            self._settings["uri"],
            socket_timeout=socket_timeout,
            socket_connect_timeout=socket_connect_timeout,
        )
        self.data_ttl = self._settings["data_ttl"]
        self.seq_ttl = self._settings["seq_ttl"]

    @property
    def client(self) -> redis.Redis:
        return self._client

    async def incr_seq(self, node_id: str) -> int:
        return await self.client.incr(f"sequence:{node_id}")

    async def set(self, node_id, sequence, metadata, payload=None):
        pipeline = self.client.pipeline()
        mapping = {
            "sequence": sequence,
            "metadata": safe_json_dump(metadata),
        }
        if payload:
            mapping["payload"] = payload
        pipeline.hset(f"data:{node_id}:{sequence}", mapping=mapping)
        pipeline.expire(f"data:{node_id}:{sequence}", self.data_ttl)
        pipeline.publish(f"notify:{node_id}", sequence)
        # Extend the lifetime of the sequence counter.
        pipeline.expire(f"sequence:{node_id}", self.seq_ttl)
        await pipeline.execute()

    async def close(self, node_id):
        # Increment the counter for this node.
        sequence = await self.incr_seq(node_id)
        # Publish a special message (end_of_stream) that will signal
        # any open clients to close.
        metadata = {
            "timestamp": datetime.now().isoformat(),
            "end_of_stream": True,
        }

        pipeline = self.client.pipeline()
        pipeline.hset(
            f"data:{node_id}:{sequence}",
            mapping={
                "sequence": sequence,
                "metadata": safe_json_dump(metadata),
            },
        )
        pipeline.expire(f"data:{node_id}:{sequence}", self.data_ttl)
        # Expire the sequence more aggressively.  It needs to outlive the last
        # piece of data for this sequence, but then it can be culled. Any
        # future writes will restart the sequence from 1.
        pipeline.expire(f"sequence:{node_id}", 1 + self.data_ttl)
        pipeline.publish(f"notify:{node_id}", sequence)
        await pipeline.execute()

    async def get(self, key, *fields):
        return await self.client.hmget(key, *fields)

    def make_ws_handler(self, websocket, formatter, uri, node_id, schema):
        async def current_sequence_getter():
            current_seq = await self.client.get(f"sequence:{node_id}")
            return int(current_seq) if current_seq is not None else 0

        async def live_sequence_source():
            pubsub = self.client.pubsub()
            await pubsub.subscribe(f"notify:{node_id}")

            async def live_iter():
                async for message in pubsub.listen():
                    if message.get("type") == "message":
                        try:
                            yield int(message["data"])
                        except Exception as e:
                            logger.exception(f"Error parsing live message: {e}")

            async def cleanup():
                await pubsub.unsubscribe(f"notify:{node_id}")
                await pubsub.aclose()

            return live_iter(), cleanup

        return _make_ws_handler_common(
            websocket=websocket,
            formatter=formatter,
            uri=uri,
            node_id=node_id,
            schema=schema,
            get_func=self.get,
            current_sequence_getter=current_sequence_getter,
            live_sequence_source=live_sequence_source,
        )
