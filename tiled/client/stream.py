import abc
import concurrent.futures
import inspect
import logging
import sys
import threading
import weakref
from typing import Callable, List, Optional

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

import anyio
import httpx
import msgpack
import websockets.exceptions
from websockets.sync.client import connect

from ..links import links_for_node
from ..stream_messages import SCHEMA_MESSAGE_TYPES, UPDATE_MESSAGE_TYPES, Schema, Update
from ..structures.core import STRUCTURE_TYPES, StructureFamily
from .context import Context
from .utils import client_for_item, normalize_specs

Callback = Callable[["Subscription", Update], None]
"A Callback will be called with the Subscription calling it and a dict with the update."


API_KEY_LIFETIME = 30  # seconds
RECEIVE_TIMEOUT = 0.1  # seconds


logger = logging.getLogger(__name__)


__all__ = ["Subscription"]


class _TestClientWebsocketWrapper:
    """Wrapper for TestClient websockets."""

    def __init__(self, http_client, uri: httpx.URL):
        self._http_client = http_client
        self._uri = uri
        self._websocket = None

    def connect(self, api_key: str, start: Optional[int] = None):
        """Connect to the websocket."""
        params = self._uri.params
        if start is not None:
            params = params.set("start", start)
        self._websocket = self._http_client.websocket_connect(
            str(self._uri.copy_with(params=params)),
            headers={"Authorization": f"Apikey {api_key}"},
        )
        self._websocket.__enter__()

    def recv(self, timeout=None):
        """Receive data from websocket with consistent interface."""
        # Hide this import because it is only used for ASGI (tests)
        # we do not want a server dependency in the client.
        import starlette.websockets

        try:
            return self._websocket.receive_bytes()
        except starlette.websockets.WebSocketDisconnect:
            return None

    def close(self):
        """Close websocket connection."""
        self._websocket.__exit__(None, None, None)


class _RegularWebsocketWrapper:
    """Wrapper for regular websockets."""

    def __init__(self, http_client, uri: httpx.URL):
        self._http_client = http_client
        self._uri = uri
        self._websocket = None

    def connect(self, api_key: str, start: Optional[int] = None):
        """Connect to the websocket."""
        params = self._uri.params
        if start is not None:
            params = params.set("start", start)
        self._websocket = connect(
            str(self._uri.copy_with(params=params)),
            additional_headers={"Authorization": f"Apikey {api_key}"},
        )

    def recv(self, timeout=None):
        """Receive data from websocket with consistent interface."""
        try:
            return self._websocket.recv(timeout=timeout)
        except websockets.exceptions.ConnectionClosedOK:
            return None

    def close(self):
        """Close websocket connection."""
        self._websocket.close()


class CallbackRegistry:
    """
    Distribute updates to user-provided callback functions.

    Parameters
    ----------

    executor : concurrent.futures.Executor
        Launches tasks asynchronously, in response to updates
    """

    def __init__(self, executor: concurrent.futures.Executor):
        self._executor = executor
        self._callbacks = set()

    @property
    def executor(self):
        return self._executor

    def process(self, sub: "Subscription", *args):
        "Fan an update out to all registered callbacks."
        for ref in self._callbacks:
            callback = ref()
            if callback is not None:
                self.executor.submit(callback, sub, *args)

    def add_callback(self, callback: Callback) -> Self:
        """
        Register a callback to be run when the Subscription receives an update.

        The callback registry only holds a weak reference to the callback. If
        no hard references are held elsewhere in the program, the callback will
        be silently removed.

        Parameters
        ----------
        callback : Callback

        Returns
        -------
        Subscription

        Examples
        --------

        Simply subscribe the print function.

        >>> sub.add_callback(print)

        Subscribe a custom function.

        >>> def f(sub, data):
                ...

        >>> sub.add_callback(f)

        Start receiving updates, beginning with the next one.

        >>> sub.start()

        Or start receiving updates beginning as far back as the server has
        available.

        >>> sub.start(0)

        Or start receiving updates beginning with a specific sequence number.

        >>> sub.start(3)

        The method calls can be chained like:

        >>> sub.add_callback(f).add_callback(g).start()

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

    def remove_callback(self, callback: Callback) -> Self:
        """
        Unregister a callback.

        Parameters
        ----------
        callback : Callback

        Returns
        -------
        Subscription
        """
        self._callbacks.remove(callback)


class Subscription(abc.ABC):
    """
    Subscribe to streaming updates from a node.

    Parameters
    ----------
    context : tiled.client.Context
        Provides connection to Tiled server
    segments : list[str]
        Path to node of interest, given as a list of path segments
    executor : concurrent.futures.Executor, optional
        Launches tasks asynchronously, in response to updates. By default,
        a concurrent.futures.ThreadPoolExecutor is used.
    """

    def __init__(
        self,
        context: Context,
        segments: List[str] = None,
        executor: Optional[concurrent.futures.Executor] = None,
    ):
        segments = segments or ["/"]
        self._context = context
        self._segments = segments
        self._executor = executor or concurrent.futures.ThreadPoolExecutor(
            max_workers=5
        )
        params = {"envelope_format": "msgpack"}
        scheme = "wss" if context.api_uri.scheme == "https" else "ws"
        self._node_path = "/".join(f"/{segment}" for segment in segments)
        uri_path = "/api/v1/stream/single" + self._node_path
        self._uri = httpx.URL(
            str(context.api_uri.copy_with(scheme=scheme, path=uri_path)),
            params=params,
        )
        self._schema = None
        self._close_event = threading.Event()
        self._thread = None
        if getattr(self.context.http_client, "app", None):
            self._websocket = _TestClientWebsocketWrapper(
                context.http_client, self._uri
            )
        else:
            self._websocket = _RegularWebsocketWrapper(context.http_client, self._uri)
        self.stream_closed = CallbackRegistry(self.executor)

    @property
    def executor(self):
        return self._executor

    def __repr__(self):
        return f"<{type(self).__name__} {self._node_path} >"

    @property
    def context(self) -> Context:
        return self._context

    @property
    def segments(self) -> List[str]:
        return self._segments

    def _connect(self, start: Optional[int] = None) -> None:
        "Connect to websocket"
        if self._close_event.is_set():
            raise RuntimeError("Cannot be restarted once stopped.")
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

        # Connect using the websocket wrapper
        self._websocket.connect(api_key, start)

        if needs_api_key:
            # The connection is made, so we no longer need the API key.
            # TODO: Implement single-use API keys so that revoking is not
            # necessary.
            self.context.revoke_api_key(key_info["first_eight"])

    def _receive(self) -> None:
        "Blocking loop that receives and processes updates"
        while not self._close_event.is_set():
            try:
                data = self._websocket.recv(timeout=RECEIVE_TIMEOUT)
            except (TimeoutError, anyio.EndOfStream):
                continue
            if data is None:
                self._close()
                return
            try:
                if self._schema is None:
                    self._schema = parse_schema(data)
                    continue
                else:
                    update = parse_update(data, self._schema)
            except Exception:
                logger.exception(
                    "A websocket message will be ignored because it could not be parsed."
                )
                continue
            self.process(update)

    @abc.abstractmethod
    def process(sub: "Subscription", *args) -> None:
        pass

    def start(self, start: Optional[int] = None) -> None:
        """
        Connect to the websocket, and block while receiving and processing updates.

        Parameters
        ----------
        start : int, optional
            By default, the stream begins from the most recent update. Use this
            parameter to replay from some earlier update. Use 1 to start from
            the first item, 0 to start from as far back as available (which may
            be later than the first item), or any positive integer to start
            from a specific point in the sequence.

        Examples
        --------

        Starting the Subscription blocks the current thread. Use Ctrl+C to interrupt.

        >>> sub.start()

        """
        self._connect(start)
        self._receive()  # blocks

    def start_in_thread(self, start: Optional[int] = None) -> Self:
        """
        Connect to the websocket, and receive and process updates on a thread.

        Parameters
        ----------
        start : int, optional
            By default, the stream begins from the most recent update. Use this
            parameter to replay from some earlier update. Use 1 to start from
            the first item, 0 to start from as far back as available (which may
            be later than the first item), or any positive integer to start
            from a specific point in the sequence.

        Examples
        --------

        Starting the Subscription connects and then starts a thread to receive
        and process updates.

        >>> sub.start()

        To stop the thread:

        >>> sub.stop()


        """
        name = f"tiled-subscription-{self._uri}"
        # Connect on the current thread, so any connection-related exceptions are
        # raised here.
        self._connect(start)
        # Run the receive loop on a thread.
        self._thread = threading.Thread(target=self._receive, daemon=True, name=name)
        self._thread.start()
        return self

    @property
    def closed(self):
        """
        Indicate whether stream has been closed.
        """
        return self._close_event.is_set()

    def _close(self) -> None:
        # This is called by the user-facing function below and also by the
        # receive loop if the server closes the connection because the stream
        # has ended.
        if self._close_event.is_set():
            return  # nothing to do
        self.stream_closed.process(self)
        self._close_event.set()
        self._websocket.close()
        self.executor.shutdown()

    def close(self) -> None:
        "Close the websocket connection."
        # If start_in_thread() was used, join the thread.
        self._close()
        if self._thread is not None:
            self._thread.join()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class ContainerSubscription(Subscription):
    """
    Subscribe to streaming updates from a container.

    Parameters
    ----------
    context : tiled.client.Context
        Provides connection to Tiled server
    segments : list[str]
        Path to node of interest, given as a list of path segments
    executor : concurrent.futures.Executor, optional
        Launches tasks asynchronously, in response to updates. By default,
        a concurrent.futures.ThreadPoolExecutor is used.
    structure_clients : dict
    """

    def __init__(
        self,
        context: Context,
        segments: List[str] = None,
        executor: Optional[concurrent.futures.Executor] = None,
        structure_clients: dict = None,
    ):
        super().__init__(context, segments, executor)
        self.structure_clients = structure_clients
        self.child_created = CallbackRegistry(self.executor)
        self.child_metadata_updated = CallbackRegistry(self.executor)

    def process(self, update: Update):
        if update.type == "container-child-created":
            # Construct a client object to represent the newly created node.
            # This has some code in common with tiled.client.container.Container.new.
            # It is unavoidably a bit fiddly. It can be improved when we are more
            # consistent about what is a parsed object and what is a dict.
            item = {
                "id": update.key,
                "attributes": {
                    "ancestors": self.segments,
                    "metadata": update.metadata,
                    "structure_family": update.structure_family,
                    "specs": normalize_specs(update.specs or []),
                    "data_sources": update.data_sources,
                    "access_blob": update.access_blob,
                },
            }
            if update.structure_family == StructureFamily.container:
                structure_for_item = {"contents": None, "count": None}
                structure_for_links = None
            else:
                (data_source,) = update.data_sources
                structure_for_item = data_source.structure
                structure_type = STRUCTURE_TYPES[item["attributes"]["structure_family"]]
                structure_for_links = structure_type.from_json(structure_for_item)
            item["attributes"]["structure"] = structure_for_item
            base_url = self.context.server_info.links["self"]
            path_str = "/".join(self.segments)
            item["links"] = links_for_node(
                update.structure_family, structure_for_links, base_url, path_str
            )

            client = client_for_item(self.context, self.structure_clients, item)
            self.child_created.process(self, client)
        elif update.type == "container-child-metadata-updated":
            self.child_metadata_updated.process(self, update)
        else:
            raise RuntimeError(f"Received update with unexpected type: {update}")


class ArraySubscription(Subscription):
    """
    Subscribe to streaming updates from an array.

    Parameters
    ----------
    context : tiled.client.Context
        Provides connection to Tiled server
    segments : list[str]
        Path to node of interest, given as a list of path segments
    executor : concurrent.futures.Executor, optional
        Launches tasks asynchronously, in response to updates. By default,
        a concurrent.futures.ThreadPoolExecutor is used.
    """

    def __init__(
        self,
        context: Context,
        segments: List[str] = None,
        executor: Optional[concurrent.futures.Executor] = None,
    ):
        super().__init__(context, segments, executor)
        self.new_data = CallbackRegistry(self.executor)

    def process(self, update: Update):
        self.new_data.process(self, update)


class UnparseableMessage(RuntimeError):
    "Message can be decoded but cannot be interpreted by the application"
    pass


def parse_schema(data: bytes) -> Schema:
    "Parse msgpack-encoded bytes into a Schema model."
    message = msgpack.unpackb(data)
    try:
        message_type = message["type"]
    except KeyError:
        raise UnparseableMessage(f"Message does not designate a 'type': {message!r}")
    try:
        cls = SCHEMA_MESSAGE_TYPES[message_type]
    except KeyError:
        raise UnparseableMessage(f"Unrecognized schema message type {message_type!r}")
    return cls(**message)


def parse_update(data: bytes, schema: Schema) -> Update:
    "Parse msgpack-encoded bytes into an Update model."
    message = msgpack.unpackb(data)
    try:
        message_type = message["type"]
    except KeyError:
        raise UnparseableMessage(f"Message does not designate a 'type': {message!r}")
    try:
        cls = UPDATE_MESSAGE_TYPES[message_type]
    except KeyError:
        raise UnparseableMessage(f"Unrecognized message type {message_type!r}")
    return cls(**message, **schema.content())
