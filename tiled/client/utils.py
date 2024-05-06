import builtins
import uuid
from collections.abc import Hashable
from pathlib import Path
from threading import Lock
from weakref import WeakValueDictionary

import httpx
import msgpack

from ..utils import path_from_uri

MSGPACK_MIME_TYPE = "application/x-msgpack"


def raise_for_status(response) -> None:
    """
    Raise the `httpx.HTTPStatusError` if one occurred. Include correlation ID.
    """
    # This is adapted from the method httpx.Response.raise_for_status, modified to
    # remove the generic link to HTTP status documentation and include the
    # correlation ID.
    request = response._request
    if request is None:
        raise RuntimeError(
            "Cannot call `raise_for_status` as the request "
            "instance has not been set on this response."
        )

    if response.is_success:
        return response

    # correlation ID may be missing if request didn't make it to the server
    correlation_id = response.headers.get("x-tiled-request-id", None)

    if response.has_redirect_location:
        message = (
            "{error_type} '{0.status_code} {0.reason_phrase}' for url '{0.url}'\n"
            "Redirect location: '{0.headers[location]}'\n"
            "For more information, server admin can search server logs for "
            "correlation ID {correlation_id}."
        )
    else:
        message = (
            "{error_type} '{0.status_code} {0.reason_phrase}' for url '{0.url}'\n"
            "For more information, server admin can search server logs for "
            "correlation ID {correlation_id}."
        )

    status_class = response.status_code // 100
    error_types = {
        1: "Informational response",
        3: "Redirect response",
        4: "Client error",
        5: "Server error",
    }
    error_type = error_types.get(status_class, "Invalid status code")
    message = message.format(
        response, error_type=error_type, correlation_id=correlation_id
    )
    raise httpx.HTTPStatusError(message, request=request, response=response)


def handle_error(response):
    if not response.is_error:
        return response
    try:
        raise_for_status(response)
    except httpx.RequestError:
        raise  # Nothing to add in this case; just raise it.
    except httpx.HTTPStatusError as exc:
        if response.status_code < httpx.codes.INTERNAL_SERVER_ERROR:
            # Include more detail that httpx does by default.
            if response.headers["Content-Type"] == "application/json":
                detail = response.json().get("detail", "")
            else:
                # This can happen when we get an error from a proxy,
                # such as a 502, which serves an HTML error page.
                # Use the stock "reason phrase" for the error code
                # instead of dumping HTML into the terminal.
                detail = response.reason_phrase
            message = f"{exc.response.status_code}: " f"{detail} " f"{exc.request.url}"
            raise ClientError(message, exc.request, exc.response) from exc
        else:
            raise


class ClientError(httpx.HTTPStatusError):
    def __init__(self, message, request, response):
        super().__init__(message=message, request=request, response=response)


class TiledResponse(httpx.Response):
    def json(self):
        if self.headers["Content-Type"] == MSGPACK_MIME_TYPE:
            return msgpack.unpackb(
                self.content,
                timestamp=3,  # Decode msgpack Timestamp as datetime.datetime object.
            )
        return super().json()


class UnknownStructureFamily(KeyError):
    pass


def export_util(file, format, get, link, params):
    """
    Download client data in some format and write to a file.

    This is used by the export method on clients. It intended for internal use.

    Parameters
    ----------
    file: str, Path, or buffer
        Filepath or writeable buffer.
    format : str, optional
        If format is None and `file` is a filepath, the format is inferred
        from the name, like 'table.csv' implies format="text/csv". The format
        may be given as a file extension ("csv") or a media type ("text/csv").
    get : callable
        Client's internal GET method
    link: str
        URL to download full data
    params : dict
        Additional parameters for the request, which may be used to subselect
        or slice, for example.
    """

    # The server accpets a media type like "text/csv" or a file extension like
    # "csv" (no dot) as a "format".
    if "format" in params:
        raise ValueError("params may not include 'format'. Use the format parameter.")
    if isinstance(format, str) and format.startswith("."):
        format = format[1:]  # e.g. ".csv" -> "csv"
    if isinstance(file, (str, Path)):
        # Infer that `file` is a filepath.
        if format is None:
            format = ".".join(
                suffix[1:] for suffix in Path(file).suffixes
            )  # e.g. "csv"
        content = handle_error(get(link, params={"format": format, **params})).read()
        with open(file, "wb") as buffer:
            buffer.write(content)
    else:
        # Infer that `file` is a writeable buffer.
        if format is None:
            # We have no filepath to infer to format from.
            raise ValueError("format must be specified when file is writeable buffer")
        content = handle_error(get(link, params={"format": format, **params})).read()
        file.write(content)


def client_for_item(
    context, structure_clients, item, structure=None, include_data_sources=False
):
    """
    Create an instance of the appropriate client class for an item.

    This is intended primarily for internal use and use by subclasses.
    """
    # The server can use specs to tell us that this is not just *any*
    # node/array/dataframe/etc. but that is matches a certain specification
    # for which there may be a special client available.
    # Check each spec in order for a matching structure client. Use the first
    # one we find. If we find no structure client for any spec, fall back on
    # the default for this structure family.
    specs = item["attributes"].get("specs", []) or []
    for spec in specs:
        class_ = structure_clients.get(spec["name"])
        if class_ is not None:
            break
    else:
        structure_family = item["attributes"]["structure_family"]
        try:
            class_ = structure_clients[structure_family]
        except KeyError:
            raise UnknownStructureFamily(structure_family) from None

    return class_(
        context=context,
        item=item,
        structure_clients=structure_clients,
        structure=structure,
        include_data_sources=include_data_sources,
    )


# These timeouts are really high, but in practice we find that
# ~100 MB chunks over very slow home Internet connections
# can bump into lower timeouts.
DEFAULT_TIMEOUT_PARAMS = {
    "connect": 5.0,
    "read": 30.0,
    "write": 30.0,
    "pool": 5.0,
}


def params_from_slice(slice):
    "Generate URL query param ?slice=... from Python slice object."
    params = {}
    if (slice is not None) and (slice is not ...):
        if isinstance(slice, (int, builtins.slice)):
            slice = [slice]
        slices = []
        for dim in slice:
            if isinstance(dim, builtins.slice):
                # slice(10, 50) -> "10:50"
                # slice(None, 50) -> ":50"
                # slice(10, None) -> "10:"
                # slice(None, None) -> ":"
                if (dim.step is not None) and dim.step != 1:
                    raise ValueError(
                        "Slices with a 'step' other than 1 are not supported."
                    )
                slices.append(
                    (
                        (str(dim.start) if dim.start else "")
                        + ":"
                        + (str(dim.stop) if dim.stop else "")
                    )
                )
            else:
                slices.append(str(int(dim)))
        params["slice"] = ",".join(slices)
    return params


class SerializableLock:
    """A Serializable per-process Lock

    Vendored from dask.utils because it is used parts of tiled that do not
    otherwise have a dask dependency.

    This wraps a normal ``threading.Lock`` object and satisfies the same
    interface.  However, this lock can also be serialized and sent to different
    processes.  It will not block concurrent operations between processes (for
    this you should look at ``multiprocessing.Lock`` or ``locket.lock_file``
    but will consistently deserialize into the same lock.
    So if we make a lock in one process::
        lock = SerializableLock()
    And then send it over to another process multiple times::
        bytes = pickle.dumps(lock)
        a = pickle.loads(bytes)
        b = pickle.loads(bytes)
    Then the deserialized objects will operate as though they were the same
    lock, and collide as appropriate.
    This is useful for consistently protecting resources on a per-process
    level.
    The creation of locks is itself not threadsafe.
    """

    _locks = WeakValueDictionary()
    token: Hashable
    lock: Lock

    def __init__(self, token=None):
        self.token = token or str(uuid.uuid4())
        if self.token in SerializableLock._locks:
            self.lock = SerializableLock._locks[self.token]
        else:
            self.lock = Lock()
            SerializableLock._locks[self.token] = self.lock

    def acquire(self, *args, **kwargs):
        return self.lock.acquire(*args, **kwargs)

    def release(self, *args, **kwargs):
        return self.lock.release(*args, **kwargs)

    def __enter__(self):
        self.lock.__enter__()

    def __exit__(self, *args):
        self.lock.__exit__(*args)

    def locked(self):
        return self.lock.locked()

    def __getstate__(self):
        return self.token

    def __setstate__(self, token):
        self.__init__(token)

    def __str__(self):
        return f"<{self.__class__.__name__}: {self.token}>"

    __repr__ = __str__


def get_asset_filepaths(node):
    """
    Given a node, return a list of filepaths of the data backing it.
    """
    filepaths = []
    for data_source in node.data_sources() or []:
        for asset in data_source.assets:
            # If, in the future, there are nodes with s3:// or other
            # schemes, path_from_uri will raise an exception here
            # because it cannot provide a filepath.
            filepaths.append(path_from_uri(asset.data_uri))
    return filepaths
