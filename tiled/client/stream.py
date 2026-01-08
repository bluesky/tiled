import abc
import concurrent.futures
import inspect
import logging
import sys
import threading
import weakref
from typing import Callable, Generic, List, Optional, TypeVar

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

import anyio
import httpx
import msgpack
import stamina
import websockets.exceptions
from pydantic import ConfigDict
from websockets.sync.client import connect

from ..client.base import BaseClient
from ..links import links_for_node
from ..media_type_registration import default_deserialization_registry
from ..stream_messages import (
    ArrayData,
    ArrayRef,
    ArraySchema,
    ChildCreated,
    ChildMetadataUpdated,
    ContainerSchema,
    Schema,
    TableData,
    TableSchema,
    Update,
)
from ..structures.core import STRUCTURE_TYPES, StructureFamily
from .context import Context
from .utils import (
    TILED_RETRY_ATTEMPTS,
    TILED_RETRY_TIMEOUT,
    client_for_item,
    handle_error,
    normalize_specs,
    retry_context,
)

T = TypeVar("T")
Callback = Callable[[T], None]

API_KEY_LIFETIME = TILED_RETRY_TIMEOUT + 15  # seconds
RECEIVE_TIMEOUT = 0.1  # seconds


logger = logging.getLogger(__name__)

__all__ = ["Subscription"]


class _TestClientWebsocketWrapper:
    """Wrapper for TestClient websockets."""

    def __init__(self, http_client, uri: httpx.URL):
        self._http_client = http_client
        self._uri = uri
        self._websocket = None
        self._connection_lock = threading.Lock()

    def connect(self, api_key: Optional[str], start: Optional[int] = None):
        """Connect to the websocket."""
        params = self._uri.params
        headers = {}
        if api_key:
            headers["Authorization"] = f"Apikey {api_key}"
        if start is not None:
            params = params.set("start", start)
        with self._connection_lock:
            self._websocket = self._http_client.websocket_connect(
                str(self._uri.copy_with(params=params)),
                headers=headers,
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
        with self._connection_lock:
            if self._websocket is not None:
                self._websocket.__exit__(None, None, None)


class _RegularWebsocketWrapper:
    """Wrapper for regular websockets."""

    def __init__(self, http_client, uri: httpx.URL):
        self._http_client = http_client
        self._uri = uri
        self._websocket = None

    def connect(self, api_key: Optional[str], start: Optional[int] = None):
        """Connect to the websocket."""
        params = self._uri.params
        headers = {}
        if api_key:
            headers["Authorization"] = f"Apikey {api_key}"
        if start is not None:
            params = params.set("start", start)
        self._websocket = connect(
            str(self._uri.copy_with(params=params)),
            additional_headers=headers,
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


class CallbackRegistry(Generic[T]):
    """
    Distribute updates to user-provided callback functions.

    Parameters
    ----------

    executor : concurrent.futures.Executor
        Launches tasks asynchronously, in response to updates
    """

    def __init__(self, executor: concurrent.futures.Executor):
        self._executor = executor
        self._callbacks: set[T] = set()

    @property
    def executor(self):
        return self._executor

    def process(self, update: T):
        "Fan an update out to all registered callbacks."
        for ref in self._callbacks:
            callback = ref()
            if callback is not None:
                self.executor.submit(callback, update)

    def add_callback(self, callback: Callback[T]) -> Self:
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

    def remove_callback(self, callback: Callback[T]) -> Self:
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
        self._disconnect_lock = threading.Lock()
        self._disconnect_event = threading.Event()
        self._thread = None
        self._last_received_sequence = None  # Track last sequence for reconnection
        if getattr(self.context.http_client, "app", None):
            self._websocket = _TestClientWebsocketWrapper(
                context.http_client, self._uri
            )
        else:
            self._websocket = _RegularWebsocketWrapper(context.http_client, self._uri)
        self.stream_closed: CallbackRegistry["Subscription"] = CallbackRegistry(
            self.executor
        )
        self.disconnected: CallbackRegistry["Subscription"] = CallbackRegistry(
            self.executor
        )

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

    def _run(self, start: Optional[int] = None) -> None:
        """Outer loop - runs for the lifecycle of the Subscription."""
        while not self._disconnect_event.is_set():
            try:
                # Resume from last received sequence if reconnecting
                start_from = (
                    self._last_received_sequence + 1
                    if self._last_received_sequence is not None
                    else start
                )
                self._connect(start_from)
                self._receive()
            except (websockets.exceptions.ConnectionClosedError, OSError):
                # Connection lost, close the websocket and reconnect
                try:
                    self._websocket.close()
                except Exception:
                    pass  # Ignore errors closing failed connection
                continue
            # Clean shutdown (no exception)
            break

    @stamina.retry(
        on=(websockets.exceptions.ConnectionClosedError, OSError),
        attempts=TILED_RETRY_ATTEMPTS,
        wait_max=TILED_RETRY_TIMEOUT,
    )
    def _connect(self, start: Optional[int] = None) -> None:
        """Connect to websocket with retry."""
        if self._disconnect_event.is_set():
            raise RuntimeError("Cannot be restarted once stopped.")

        # Reset schema so first message on new connection is parsed as schema
        self._schema = None

        needs_api_key = self.context.server_info.authentication.providers
        if needs_api_key:
            # Request a short-lived API key to use for authenticating the WS connection.
            key_info = self.context.create_api_key(
                expires_in=API_KEY_LIFETIME, note="websocket"
            )
            api_key = key_info["secret"]
        else:
            # Use single-user API key or None (if unauthenticated).
            api_key = self.context.api_key

        # Connect using the websocket wrapper
        self._websocket.connect(api_key, start)

        if needs_api_key:
            # The connection is made, so we no longer need the API key.
            # TODO: Implement single-use API keys so that revoking is not
            # necessary.
            self.context.revoke_api_key(key_info["first_eight"])

    def _receive(self) -> None:
        """Receive and process websocket messages."""
        while not self._disconnect_event.is_set():
            try:
                data = self._websocket.recv(timeout=RECEIVE_TIMEOUT)
            except (TimeoutError, anyio.EndOfStream):
                continue
            # Let ConnectionClosedError and OSError propagate to _run()

            if data is None:
                self.stream_closed.process(self)
                self._disconnect()
                return

            try:
                if self._schema is None:
                    self._schema = parse_schema(data)
                    continue
                else:
                    update = parse_update(self, data, self._schema)
            except Exception:
                logger.exception(
                    "A websocket message will be ignored because it could not be parsed."
                )
                continue

            self._last_received_sequence = update.sequence
            self.process(update)

    @abc.abstractmethod
    def process(self, *args) -> None:
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
        try:
            self._run(start)  # blocks
        finally:
            self.disconnect()

    def start_in_thread(self, start: Optional[int] = None) -> Self:
        """
        Start a thread to connect to the websocket and receive updates.

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

        Starting the Subscription starts a thread that connects and receives updates.

        >>> sub.start_in_thread()

        To stop the thread:

        >>> sub.disconnect()


        """
        name = f"tiled-subscription-{self._uri}"
        self._thread = threading.Thread(
            target=self._run,
            args=(start,),
            daemon=True,
            name=name,
        )
        self._thread.start()
        return self

    def _disconnect(self, wait=True) -> None:
        # This is called by the user-facing disconnect method below and also by
        # the receive loop if the server closes the connection because the
        # stream has ended.
        with self._disconnect_lock:
            if self._disconnect_event.is_set():
                return  # nothing to do
            self._disconnect_event.set()
        try:
            self._websocket.close()
        except Exception:
            # Websocket may not have been fully connected
            pass
        self.disconnected.process(self)
        self.executor.shutdown(wait=True)

    def disconnect(self, wait=True) -> None:
        "Close the websocket connection."
        self._disconnect(wait=True)
        # If start_in_thread() was used, join the thread.
        if wait and (self._thread is not None):
            self._thread.join()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.disconnect()


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
        self.child_created: CallbackRegistry["LiveChildCreated"] = CallbackRegistry(
            self.executor
        )
        self.child_metadata_updated: CallbackRegistry[
            "LiveChildMetadataUpdated"
        ] = CallbackRegistry(self.executor)

    def process(self, update: Update):
        if update.type == "container-child-created":
            self.child_created.process(update)
        elif update.type == "container-child-metadata-updated":
            self.child_metadata_updated.process(update)
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
        self.new_data: CallbackRegistry[
            "LiveArrayData" | "LiveArrayRef"
        ] = CallbackRegistry(self.executor)

    def process(self, update: Update):
        self.new_data.process(update)


class TableSubscription(Subscription):
    """
    Subscribe to streaming updates from an table.

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
        self.new_data: CallbackRegistry["LiveTableData"] = CallbackRegistry(
            self.executor
        )

    def process(self, update: Update):
        self.new_data.process(update)


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


def parse_update(subscription: Subscription, data: bytes, schema: Schema) -> Update:
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
    return cls(subscription=subscription, **message, **schema.content())


class LiveChildCreated(ChildCreated):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    subscription: ContainerSubscription

    def child(self) -> BaseClient:
        "Construct a client object for the child."
        # Construct a client object to represent the newly created node.
        # This has some code in common with tiled.client.container.Container.new.
        # It is unavoidably a bit fiddly. It can be improved when we are more
        # consistent about what is a parsed object and what is a dict.
        item = {
            "id": self.key,
            "attributes": {
                "ancestors": self.subscription.segments,
                "metadata": self.metadata,
                "structure_family": self.structure_family,
                "specs": normalize_specs(self.specs or []),
                "data_sources": self.data_sources,
                "access_blob": self.access_blob,
            },
        }
        if self.structure_family == StructureFamily.container:
            structure_for_item = {"contents": None, "count": None}
            structure_for_links = None
        else:
            (data_source,) = self.data_sources
            structure_for_item = data_source.structure
            structure_type = STRUCTURE_TYPES[item["attributes"]["structure_family"]]
            structure_for_links = structure_type.from_json(structure_for_item)
        item["attributes"]["structure"] = structure_for_item
        context = self.subscription.context
        base_url = context.server_info.links["self"]
        path_str = "/".join(self.subscription.segments + [self.key])
        item["links"] = links_for_node(
            self.structure_family, structure_for_links, base_url, path_str
        )

        return client_for_item(context, self.subscription.structure_clients, item)


class LiveChildMetadataUpdated(ChildMetadataUpdated):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    subscription: ContainerSubscription


class LiveArrayData(ArrayData):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    subscription: ArraySubscription

    def data(self):
        "Decode array"
        # Registration occurs on import. Ensure this is imported.
        from ..serialization import array

        del array

        # Decode payload (bytes) into array.
        deserializer = default_deserialization_registry.dispatch("array", self.mimetype)
        return deserializer(self.payload, self.data_type.to_numpy_dtype(), self.shape)


class LiveArrayRef(ArrayRef):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    subscription: ArraySubscription

    def data(self):
        "Fetch array"
        import numpy

        for attempt in retry_context():
            with attempt:
                content = handle_error(
                    self.subscription.context.http_client.get(
                        self.uri,
                        headers={"Accept": "application/octet-stream"},
                    )
                ).read()
        # Decode payload (bytes) into array.
        numpy_dtype = self.data_type.to_numpy_dtype()
        if self.patch:
            shape = self.patch.shape
        else:
            shape = self.shape
        return numpy.frombuffer(content, dtype=numpy_dtype).reshape(shape)


class LiveTableData(TableData):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    subscription: TableSubscription

    def data(self):
        "Get table"
        # Registration occurs on import. Ensure this is imported.
        from ..serialization import table

        del table

        # Decode payload (bytes) into table.
        deserializer = default_deserialization_registry.dispatch("table", self.mimetype)
        return deserializer(self.payload)


SCHEMA_MESSAGE_TYPES = {
    "array-schema": ArraySchema,
    "container-schema": ContainerSchema,
    "table-schema": TableSchema,
}
UPDATE_MESSAGE_TYPES = {
    "container-child-created": LiveChildCreated,
    "container-child-metadata-updated": LiveChildMetadataUpdated,
    "array-data": LiveArrayData,
    "array-ref": LiveArrayRef,
    "table-data": LiveTableData,
}
