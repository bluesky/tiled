import builtins
import collections
import contextlib
import os
from pathlib import Path

import httpx

from ..utils import modules_available

# By default, the token in the authentication header is redacted from the logs.
# Set thie env var to 1 to show it for debugging purposes.
TILED_LOG_AUTH_TOKEN = int(os.getenv("TILED_LOG_AUTH_TOKEN", False))


DEFAULT_ACCEPTED_ENCODINGS = ["gzip"]
if modules_available("blosc"):
    DEFAULT_ACCEPTED_ENCODINGS.append("blosc")


def handle_error(response):
    if not response.is_error:
        return
    try:
        response.raise_for_status()
    except httpx.RequestError:
        raise  # Nothing to add in this case; just raise it.
    except httpx.HTTPStatusError as exc:
        if response.status_code < 500:
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


class NotAvailableOffline(Exception):
    "Item looked for in offline cache was not found."


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
        content = get(link, params={"format": format, **params})
        with open(file, "wb") as buffer:
            buffer.write(content)
    else:
        # Infer that `file` is a writeable buffer.
        if format is None:
            # We have no filepath to infer to format from.
            raise ValueError("format must be specified when file is writeable buffer")
        content = get(link, params={"format": format, **params})
        file.write(content)


if __debug__:

    import logging

    class ClientLogRecord(logging.LogRecord):
        def getMessage(self):
            if hasattr(self, "request"):
                request = self.request
                message = f"-> {request.method} '{request.url}' " + " ".join(
                    f"'{k}:{v}'"
                    for k, v in request.headers.items()
                    if k != "authorization"
                )
                # Handle the authorization header specially.
                # For debugging, it can be useful to show it so that the log message
                # can be copy/pasted and passed to httpie in a shell.
                # But for screen-sharing demos, it should be redacted.
                if TILED_LOG_AUTH_TOKEN:
                    if "authorization" in request.headers:
                        message += (
                            f" 'authorization:{request.headers['authorization']}'"
                        )
                else:
                    if "authorization" in request.headers:
                        scheme, _, param = request.headers["authorization"].partition(
                            " "
                        )
                        message += f" 'authorization:{scheme} [redacted]'"
            elif hasattr(self, "response"):
                response = self.response
                request = response.request
                message = f"<- {response.status_code} " + " ".join(
                    f"{k}:{v}" for k, v in response.headers.items()
                )
            else:
                message = super().getMessage()
            return message

    def patched_make_record(
        name,
        level,
        fn,
        lno,
        msg,
        args,
        exc_info,
        func=None,
        extra=None,
        sinfo=None,
    ):
        rv = ClientLogRecord(name, level, fn, lno, msg, args, exc_info, func, sinfo)
        if extra is not None:
            for key in extra:
                if (key in ["message", "asctime"]) or (key in rv.__dict__):
                    raise KeyError("Attempt to overwrite %r in LogRecord" % key)
                rv.__dict__[key] = extra[key]
        return rv

    logger = logging.getLogger("tiled.client")
    # Monkey-patch our logger!
    # The logging framework provides no way to look a custom record factory into
    # the global logging manager. I tried several ways to avoid monkey-patching
    # and this is the least bad. Notice that it only downloades the 'tiled.client'
    # logger and will not affect the behavior of other loggers.
    logger.makeRecord = patched_make_record
    handler = logging.StreamHandler()
    log_format = "%(asctime)s.%(msecs)03d %(message)s"

    handler.setFormatter(logging.Formatter(log_format, datefmt="%H:%M:%S"))

    def log_request(request):
        logger.debug("", extra={"request": request})

    def log_response(response):
        logger.debug("", extra={"response": response})

    async def async_log_request(request):
        return log_request(request)

    async def async_log_response(response):
        return log_response(response)

    def collect_request(request):
        if _history is not None:
            _history.requests.append(request)

    def collect_response(response):
        if _history is not None:
            _history.responses.append(response)

    async def async_collect_request(request):
        return collect_request(request)

    async def async_collect_response(response):
        return collect_response(response)

    EVENT_HOOKS = {
        "request": [log_request, collect_request],
        "response": [log_response, collect_response],
    }
    ASYNC_EVENT_HOOKS = {
        "request": [async_log_request, async_collect_request],
        "response": [async_log_response, async_collect_response],
    }
else:
    # We take this path when Python is started with -O optimizations.
    ASYNC_EVENT_HOOKS = EVENT_HOOKS = {"request": [], "response": []}


def show_logs():
    """
    Log network traffic and interactions with the cache.

    This is just a convenience function that makes some Python logging configuration calls.
    """
    logger.setLevel("DEBUG")
    logger.addHandler(handler)


def hide_logs():
    """
    Undo show_logs().
    """
    logger.setLevel("WARNING")
    if handler in logger.handlers:
        logger.removeHandler(handler)


History = collections.namedtuple("History", "requests responses")
_history = None


@contextlib.contextmanager
def record_history():
    """
    Collect requests and responses.

    >>> with history() as t:
    ...     ...
    ...

    >>> t.requests
    [...]

    >>> t.responses
    [...]
    """
    global _history

    responses = []
    requests = []
    history = History(requests, responses)
    _history = history
    yield history
    _history = None


def client_for_item(context, structure_clients, item, path, structure=None):
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
        class_ = structure_clients.get(spec)
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
        path=path,
        structure_clients=structure_clients,
        structure=structure,
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
    if slice is not None:
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
