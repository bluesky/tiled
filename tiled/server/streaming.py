import asyncio
import itertools
import logging
import weakref
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
    def __init__(self, cache_config: dict[str, Any]) -> None:
        self._config = cache_config
        datastore_name = (self._config.get("datastore", "")).lower()
        if not datastore_name:
            raise ValueError("backend not specified in streaming_cache_config")
        try:
            datastore_cls = _DATASTORES[datastore_name]
        except KeyError:
            raise ValueError(
                f"Unknown backend '{datastore_name}'. Available backends: {sorted(_DATASTORES)}"
            )
        self._datastore = datastore_cls(self._config)

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
    """
    Lightweight in-process publish/subscribe mechanism for TTLCache backend.

    This class provides a simple pub/sub system for use within a single process,
    intended as a backend for streaming data (e.g., with TTLCache). It allows
    multiple subscribers to listen for messages on named topics.

    Subscribers are tracked using weak references to asyncio.Queue objects.
    When a subscriber is garbage collected or unsubscribed, its weak reference
    is automatically removed from the topic's subscriber set, ensuring that
    resources are cleaned up without manual intervention.

    Thread-safety: This class is NOT thread-safe and is intended for use within
    a single asyncio event loop (i.e., within a single thread).

    Comparison to Redis pub/sub:
        - This implementation is in-process only; it does not support
          cross-process or cross-machine communication.
        - It is lightweight and has no external dependencies beyond Python's
          standard library and asyncio.
        - Unlike Redis pub/sub, messages are only delivered to subscribers
          within the same process.
    """

    def __init__(self):
        self._topics: dict[str, set[weakref.ref[asyncio.Queue]]] = defaultdict(set)

    def _cleanup(self, topic: str, ref: weakref.ref) -> None:
        # Remove references that were GC'd or explicitly unsubscribed.
        topic_subscribers = self._topics.get(topic)
        if topic_subscribers is None:
            return
        topic_subscribers.discard(ref)
        if not topic_subscribers:
            self._topics.pop(topic, None)

    async def publish(self, topic: str, message):
        for ref in list(self._topics.get(topic, ())):
            q = ref()
            if q:
                try:
                    q.put_nowait(message)
                except asyncio.QueueFull as e:
                    logger.exception(
                        f"Queue full while adding message to topic, {topic}: {e}",
                    )

    def subscribe(self, topic: str):
        q = asyncio.Queue()
        self_ref = weakref.ref(self)

        def _cleanup_cb(_ref: Optional[weakref.ref] = None):
            # The weakref callback must not strongly reference self to avoid a reference cycle.
            self_obj = self_ref()
            if self_obj is None:
                return
            self_obj._cleanup(topic, _ref or ref)

        # Cleanup runs only when the queue is GC'd via the weakref callback.
        ref = weakref.ref(q, _cleanup_cb)
        self._topics[topic].add(ref)

        async def gen():
            while True:
                yield await q.get()

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
    """
    Create a websocket handler that implements the streaming protocol for a node.

    Parameters
    ----------
    websocket : fastapi.WebSocket
        The websocket connection to communicate with the client.
    formatter : Callable[[Any, Any, Optional[int]], Awaitable[None]]
        Async function to serialize and send data to the websocket.
        Typically, it takes (websocket, data, sequence).
    uri : str
        The URI identifying the resource being streamed.
    node_id : str
        The unique identifier for the node whose data is being streamed.
    schema : Any
        The schema object describing the structure of the streamed data.
    get_func : Callable[..., Any]
        Function to retrieve data for a given sequence number. Signature: get_func(node_id, sequence, ...).
    current_sequence_getter : Callable[[], Any]
        Function to get the current/latest sequence number for the node.
    live_sequence_source : Callable[[], Tuple[AsyncIterator[int], Optional[Callable[[], Awaitable[None]]]]]
        Function returning an async iterator of new sequence numbers as they become available,
        and optionally a cleanup callback to be awaited when the stream ends.

    Returns
    -------
    handler : Callable[[Optional[int]], Awaitable[None]]
        An async function that, when called with an optional starting sequence number,
        handles the websocket protocol for streaming data to the client.

    Protocol Flow
    -------------
    1. Sends the schema to the client to provide context for interpreting subsequent data.
    2. If a starting sequence is provided, replays historical data from that sequence up to the current sequence.
    3. Streams new data live as it becomes available.

    Error Handling
    --------------
    - Handles websocket disconnects gracefully.
    - Catches and logs exceptions during streaming; closes the websocket on error.
    - Ensures cleanup of resources (e.g., live stream subscriptions) on exit.
    """

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
                # This means that the data is no longer available (either expired or not found)
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

            await websocket.close(code=1000, reason="Producer ended stream")
        except WebSocketDisconnect:
            logger.info(f"Client disconnected from node {node_id}")
        finally:
            live_task.cancel()
            await live_task

    return handler


@register_datastore("ttlcache")
class TTLCacheDatastore(StreamingDatastore):
    """
    An in-memory, TTL-based streaming datastore for single-process deployments.

    This class provides a drop-in, in-memory alternative to the Redis-backed
    streaming datastore. It is intended for development, testing, or
    single-process production deployments where persistence and multi-process
    support are not required.

    Configuration:
        settings: dict with the following keys:
            - maxsize (int, optional): Maximum number of items to cache per node.
              Defaults to 1000.
            - seq_ttl (float): Time-to-live (in seconds) for sequence counters.
            - data_ttl (float): Time-to-live (in seconds) for data entries.

    Limitations:
        - Not suitable for multi-process or distributed deployments.
        - No persistence: all data is lost when the process exits.
        - Not safe for use with multiple server processes or workers.

    When to use:
        - Use this class for local development, testing, or simple single-process
          deployments where persistence and multi-process support are not needed.
        - For production or distributed deployments, use the Redis-backed
          StreamingDatastore instead.
    """

    def __init__(self, settings: Dict[str, Any]):
        self._settings = settings
        self._lock = asyncio.Lock()
        maxsize = self._settings.get("maxsize", 1000)
        seq_ttl = self._settings.get("seq_ttl", 3600)
        self._seq_cache = cachetools.TTLCache(
            maxsize=maxsize,
            ttl=seq_ttl,
        )
        self._seq_counters = cachetools.TTLCache(
            maxsize=maxsize,
            ttl=seq_ttl,
        )
        self._data_cache = cachetools.TTLCache(
            maxsize=maxsize, ttl=self._settings.get("data_ttl", 2592000)
        )
        self._pubsub = PubSub()

    @property
    def client(self):
        return self

    async def incr_seq(self, node_id: str) -> int:
        async with self._lock:
            counter = self._seq_counters.get(node_id)
            if counter is None:
                counter = itertools.count(1)
                self._seq_counters[node_id] = counter
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
        async with self._lock:
            self._data_cache[f"data:{node_id}:{sequence}"] = mapping
        await self._pubsub.publish(f"notify:{node_id}", sequence)

    async def get(self, key, *fields):
        async with self._lock:
            mapping = self._data_cache.get(key)
        if mapping is None:
            return [None for _ in fields]
        return [mapping.get(field) for field in fields]

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
        async with self._lock:
            self._data_cache[f"data:{node_id}:{sequence}"] = mapping
        await self._pubsub.publish(f"notify:{node_id}", sequence)

    def make_ws_handler(self, websocket, formatter, uri, node_id, schema):
        async def current_sequence_getter():
            async with self._lock:
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
