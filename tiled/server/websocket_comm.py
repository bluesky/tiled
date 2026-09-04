import asyncio
import logging
from typing import Any

from fastapi import WebSocket
import msgpack
from starlette.requests import URL

from tiled.server.core import get_websocket_envelope_formatter
from tiled.server.dependencies import get_entry
from tiled.server.schemas import EnvelopeFormat
from tiled.server.utils import get_base_url_websocket
from tiled.stream_messages import CompleteMsg, SubscribeMsg, WebsocketMsg, websocket_msg_adapter, ErrorMsg, Ping, Pong
from tiled.structures.core import StructureFamily


logger = logging.getLogger(__name__)




class WebSocketConnectionManager:
    def __init__(self, websocket: WebSocket, envelope_format: EnvelopeFormat, principal, authn_access_tags, authn_scopes, deserialization_registry):
        self.websocket = websocket
        self.envelope_format = envelope_format
        self.principal = principal
        self.authn_access_tags = authn_access_tags
        self.authn_scopes = authn_scopes
        self.deserialization_registry = deserialization_registry
        # Buffers should only contain pydantic models
        self.send_buffer: asyncio.Queue[Any] = asyncio.Queue()
        self.recv_buffer: asyncio.Queue[WebsocketMsg] = asyncio.Queue()
        # Keep track of live streaming tasks keyed by path
        self.streaming_tasks: dict[str, asyncio.Task] = {}

    async def send_messages(self):
        """
          This method's only job is to send messages from buffer into WS.
          Potentially implement back-pressure code here?  
        """
        while True:
            message = await self.send_buffer.get()
            match self.envelope_format:
                case EnvelopeFormat.msgpack:
                    message = msgpack.packb(message.model_dump(mode="python"))
                    await self.websocket.send_bytes(message)
                case EnvelopeFormat.json:
                    message = message.model_dump_json()
                    await self.websocket.send_text(message)

    async def recv_messages(self):
        """
          This method only receives messages and puts it in a buffer  
        """
        while True:
            raw_message = await self.websocket.receive()
            match raw_message["type"]:
                case "websocket.receive":
                    if (json_message:=raw_message.get("text")) is not None:
                        try:
                            message = websocket_msg_adapter.validate_strings(json_message)
                        except Exception:
                            await self.send_buffer.put(ErrorMsg(error="Could not decode JSON message"))
                    elif (bytes_message:=raw_message.get("bytes")) is not None:
                        try:
                            message = websocket_msg_adapter.validate_python(msgpack.unpackb(bytes_message))
                        except Exception:
                            await self.send_buffer.put(ErrorMsg(error="Could not decode msgpack message"))
                    await self.recv_buffer.put(message)
                case "websocket.disconnect":
                    break


    async def setup_stream(self, message: SubscribeMsg):
        """
          This function creates a task for each node subscribed.
          Re-uses most of the code written for single websocket stream, but
          instead of pushing data to a websocket connection, it pushes to send buffer
        """
        entry = await get_entry(
            message.path,
            ["read:data", "read:metadata"],
            self.principal,
            self.authn_access_tags,
            self.authn_scopes,
            self.websocket.app.state.root_tree,
            {},
            self.websocket.state.metrics,
            {
                StructureFamily.array,
                StructureFamily.container,
                StructureFamily.ragged,
                StructureFamily.sparse,
                StructureFamily.table,
            },
            getattr(self.websocket.app.state, "access_policy", None),
        )
        formatter = get_websocket_envelope_formatter(
            self.envelope_format, entry, self.deserialization_registry
        )
        base_websocket_url = URL(get_base_url_websocket(self.websocket))
        scheme = "https" if base_websocket_url.scheme == "wss" else "http"
        path_parts = [segment for segment in message.path.split("/") if segment]
        path_str = "/".join(path_parts)
        uri = f"{base_websocket_url.replace(scheme=scheme)}/array/full/{path_str}"
        handler = entry.make_ws_handler(self.send_buffer, formatter, uri)
        self.streaming_tasks[message.path] = asyncio.create_task(handler(message.start, already_accepted=True))
        

    async def process_messages(self):
        """
          Gets messages from the receive buffer and figures out what to do with it  
        """
        while True:
            message = await self.recv_buffer.get()
            match message:
                case Ping():
                    await self.send_buffer.put(Pong(payload=message.payload))
                case Pong():
                    logger.info("Received pong with payload: %s", message.payload)
                case SubscribeMsg():
                    await self.setup_stream(message)
                case CompleteMsg():
                    task = self.streaming_tasks.get(message.path)
                    if task is not None:
                        task.cancel()
                        self.streaming_tasks.pop(message.path, None)


    async def run(self):
        """
          Setup and teardown of all async tasks run by this class.
          By the time you're here, the assumption is that the websocket connection is already
          authenticated.
          No auth happens in this class  
        """
        tasks = {
            asyncio.create_task(self.send_messages()),
            asyncio.create_task(self.recv_messages()),
            asyncio.create_task(self.process_messages())
        }

        try:
            await asyncio.wait(
                tasks,
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            all_tasks = (*tasks, *self.streaming_tasks.values())
            for task in all_tasks:
                task.cancel()
            await asyncio.gather(*all_tasks, return_exceptions=True)
            

