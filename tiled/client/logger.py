import collections
import contextlib
import logging
import os

from ..utils import bytesize_repr

# By default, the token in the authentication header is redacted from the logs.
# Set this env var to 1 to show it for debugging purposes.
TILED_LOG_AUTH_TOKEN = int(os.getenv("TILED_LOG_AUTH_TOKEN", False))


class ClientLogRecord(logging.LogRecord):
    def getMessage(self):
        if hasattr(self, "request"):
            request = self.request
            if "content-length" in request.headers:
                size = f"({bytesize_repr(int(request.headers['content-length']))})"
            else:
                size = ""
            message = f"-> {size} {request.method} '{request.url}' " + " ".join(
                f"'{k}:{v}'" for k, v in request.headers.items() if k != "authorization"
            )
            # Handle the authorization header specially.
            # For debugging, it can be useful to show it so that the log message
            # can be copy/pasted and passed to httpie in a shell.
            # But for screen-sharing demos, it should be redacted.
            if TILED_LOG_AUTH_TOKEN:
                if "authorization" in request.headers:
                    message += f" 'authorization:{request.headers['authorization']}'"
            else:
                if "authorization" in request.headers:
                    scheme, _, param = request.headers["authorization"].partition(" ")
                    message += f" 'authorization:{scheme} [redacted]'"
        elif hasattr(self, "response"):
            response = self.response
            request = response.request
            if "content-length" in response.headers:
                size = f"({bytesize_repr(int(response.headers['content-length']))})"
            else:
                size = ""
            message = f"<- {size} {response.status_code} " + " ".join(
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
# and this is the least bad. Notice that it only downloads the 'tiled.client'
# logger and will not affect the behavior of other loggers.
logger.makeRecord = patched_make_record
handler = logging.StreamHandler()
log_format = "%(asctime)s.%(msecs)03d %(message)s"

handler.setFormatter(logging.Formatter(log_format, datefmt="%H:%M:%S"))


def log_request(request):
    logger.debug("", extra={"request": request})


def log_response(response):
    logger.debug("", extra={"response": response})


def collect_request(request):
    if _history is not None:
        _history.requests.append(request)


def collect_response(response):
    if _history is not None:
        _history.responses.append(response)


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
