import threading
import weakref
from typing import List

import httpx
import msgpack
from websockets.sync.client import connect

from tiled.client.context import Context


class Subscription:
    def __init__(self, context: Context, segments: List[str] = None, start: int = None):
        self._context = context
        self._segments = segments
        segments = segments or ["/"]
        params = {"envelope_format": "msgpack"}
        if start is not None:
            params["start"] = start
        scheme = "wss" if context.api_uri == "https" else "ws"
        path = "stream/single" + "/".join(f"/{segment}" for segment in segments)
        self._uri = httpx.URL(
            str(context.api_uri.copy_with(scheme=scheme)) + path,
            params=params,
        )
        name = f"tiled-subscription-{self._uri}"
        self._thread = threading.Thread(target=self._receive, daemon=True, name=name)
        self._callbacks = weakref.WeakSet()
        self._close_event = threading.Event()

    @property
    def context(self):
        return self._context

    @property
    def segments(self):
        return self._segments

    def add_callback(self, callback):
        self._callbacks.add(callback)

    def remove_callback(self, callback):
        self._callbacks.remove(callback)

    def _receive(self):
        TIMEOUT = 0.1  # seconds
        while not self._close_event.is_set():
            try:
                data_bytes = self._websocket.recv(timeout=TIMEOUT)
            except TimeoutError:
                continue
            data = msgpack.unpackb(data_bytes)
            for callback in self._callbacks:
                callback(self, data)

    def start(self):
        if self._close_event.is_set():
            raise RuntimeError("Cannot be restarted once stopped.")
        API_KEY_LIFETIME = 30  # seconds
        if self.context.server_info.authentication.providers:
            # Request API key.
            key_info = self.context.create_api_key(
                expires_in=API_KEY_LIFETIME, note="websocket"
            )
            api_key = key_info["secret"]
        else:
            # Use single-user API key.
            api_key = self.context.api_key
        self._websocket = connect(
            str(self._uri), additional_headers={"Authorization": api_key}
        )
        self._thread.start()

    def stop(self):
        self._close_event.set()
