import asyncio
import logging
from datetime import datetime
from typing import Any, Callable, Dict, Optional, Protocol

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
    def client(self) -> Any:
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
        async def handler(sequence: Optional[int] = None):
            await websocket.accept()
            end_stream = asyncio.Event()
            streaming_cache = self

            # Send schema to provide client context to interpret what follows.
            await formatter(websocket, schema, None)

            async def stream_data(sequence):
                """Helper function to stream a specific sequence number to a websocket"""
                key = f"data:{node_id}:{sequence}"
                payload_bytes, metadata_bytes = await streaming_cache.get(
                    key, "payload", "metadata"
                )
                if metadata_bytes is None:
                    # This means that redis ttl has expired for this sequence
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
                pubsub = streaming_cache.client.pubsub()
                await pubsub.subscribe(f"notify:{node_id}")
                try:
                    async for message in pubsub.listen():
                        if message.get("type") == "message":
                            try:
                                live_seq = int(message["data"])
                                await stream_buffer.put(live_seq)
                            except Exception as e:
                                logger.exception(f"Error parsing live message: {e}")
                except Exception as e:
                    logger.exception(f"Live subscription error: {e}")
                finally:
                    await pubsub.unsubscribe(f"notify:{node_id}")
                    await pubsub.aclose()

            live_task = asyncio.create_task(buffer_live_events())

            if sequence is not None:
                # If a sequence number is passed, replay old data
                current_seq = await streaming_cache.client.get(f"sequence:{node_id}")
                current_seq = int(current_seq) if current_seq is not None else 0
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

        return handler
