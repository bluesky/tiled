import inspect
import threading
import weakref
from typing import Callable, List

import httpx
import msgpack
from websockets.sync.client import connect

from tiled.client.context import Context

Callback = Callable[["Subscription", dict], None]
"A Callback will be called with the Subscription calling it and a dict with the update."


class Subscription:
    """
    Subscribe to streaming updates from a node.

    Parameters
    ----------
    context : tiled.client.Context
        Provides connection to Tiled server
    segments : list[str]
        Path to node of interest, given as a list of path segments
    start : int, optional
        By default, the stream begins from the most recent update. Use this parameter
        to replay from some earlier update. Use 1 to start from the first item, 0
        to start from as far back as available (which may be later than the first item),
        or any positive integer to start from a specific point in the sequence.
    """

    def __init__(self, context: Context, segments: List[str] = None, start: int = None):
        segments = segments or ["/"]
        self._context = context
        self._segments = segments
        params = {"envelope_format": "msgpack"}
        if start is not None:
            params["start"] = start
        scheme = "wss" if context.api_uri.scheme == "https" else "ws"
        path = "stream/single" + "/".join(f"/{segment}" for segment in segments)
        self._uri = httpx.URL(
            str(context.api_uri.copy_with(scheme=scheme)) + path,
            params=params,
        )
        name = f"tiled-subscription-{self._uri}"
        self._thread = threading.Thread(target=self._receive, daemon=True, name=name)
        self._callbacks = set()
        self._close_event = threading.Event()

    @property
    def context(self) -> Context:
        return self._context

    @property
    def segments(self) -> List[str]:
        return self._segments

    def add_callback(self, callback: Callback) -> None:
        """
        Register a callback to be run when the Subscription receives an update.

        The callback registry only holds a weak reference to the callback. If
        no hard references are held elsewhere in the program, the callback will
        be silently removed.

        Examples
        --------

        Simply subscribe the print function.

        >>> sub.add_callback(print)

        Subscribe a custom function.

        >>> def f(sub, data):
                ...

        >>> sub.add_callback(f)
        """

        def cleanup(ref: weakref.ref) -> None:
            # When an object is garbage collected, remove its entry
            # from the set of callbacks.
            self._callbacks.remove(ref)

        if inspect.ismethod(callback):
            # This holds the reference to the method until the object it is
            # bound to is garbage collected.
            ref = weakref.WeakMethod(callback, cleanup)
        else:
            ref = weakref.ref(callback, cleanup)
        self._callbacks.add(ref)

    def remove_callback(self, callback: Callback) -> None:
        """
        Unregister a callback.
        """
        self._callbacks.remove(callback)

    def _receive(self) -> None:
        "This method is executed on self._thread."
        TIMEOUT = 0.1  # seconds
        while not self._close_event.is_set():
            try:
                data_bytes = self._websocket.recv(timeout=TIMEOUT)
            except TimeoutError:
                continue
            data = msgpack.unpackb(data_bytes)
            for ref in self._callbacks:
                callback = ref()
                if callback is not None:
                    callback(self, data)

    def start(self) -> None:
        "Connect to the websocket and launch a thread to receive and process updates."
        if self._close_event.is_set():
            raise RuntimeError("Cannot be restarted once stopped.")
        API_KEY_LIFETIME = 30  # seconds
        needs_api_key = self.context.server_info.authentication.providers
        if needs_api_key:
            # Request a short-lived API key to use for authenticating the WS connection.
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
        if needs_api_key:
            # The connection is made, so we no longer need the API key.
            # TODO: Implement single-use API keys so that revoking is not
            # necessary.
            self.context.revoke_api_key(key_info["first_eight"])
        self._thread.start()

    def stop(self) -> None:
        "Close the websocket connection."
        self._close_event.set()
        self._thread.join()
        self._websocket.close()
