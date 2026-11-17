import urllib.parse
from copy import copy
from logging import Filter, LogRecord

from uvicorn.logging import AccessFormatter as _UvicornAccessFormatter

from .utils import get_trace_id, request_trace_id


class TraceIdFilter(Filter):
    """Logging filter to attach the request's OpenTelemetry trace ID to LogRecord.

    Sets `trace_id` (32 hex digits, or "-" if the request is not traced) for
    custom formats, and `trace_id_suffix` (" <trace ID>", or "" if not traced),
    which the default formats append to the correlation ID so that log lines
    are unchanged when tracing is off.
    """

    def filter(self, record: LogRecord) -> bool:
        trace_id = get_trace_id() or request_trace_id.get()
        record.trace_id = trace_id or "-"
        record.trace_id_suffix = f" {trace_id}" if trace_id else ""
        return True


class AccessFormatter(_UvicornAccessFormatter):
    """Uvicorn AccessFormatter that decodes percent-encoded URLs in logs."""

    def formatMessage(self, record: LogRecord) -> str:
        recordcopy = copy(record)
        (
            client_addr,
            method,
            full_path,
            http_version,
            status_code,
        ) = recordcopy.args  # type: ignore[misc]
        # Decode percent-encoded characters for readability
        full_path = urllib.parse.unquote(full_path)
        recordcopy.args = (client_addr, method, full_path, http_version, status_code)
        return super().formatMessage(recordcopy)


LOGGING_CONFIG = {
    "disable_existing_loggers": False,
    "filters": {
        "principal": {
            "()": "tiled.server.principal_log_filter.PrincipalFilter",
        },
        "correlation_id": {
            "()": "asgi_correlation_id.CorrelationIdFilter",
            "default_value": "-",
            "uuid_length": 16,
        },
        "trace_id": {
            "()": "tiled.server.logging_config.TraceIdFilter",
        },
    },
    "formatters": {
        "access": {
            "()": "tiled.server.logging_config.AccessFormatter",
            "datefmt": "%Y-%m-%dT%H:%M:%S",
            "format": (
                "[%(correlation_id)s%(trace_id_suffix)s] "
                '%(client_addr)s (%(principal)s) - "%(request_line)s" '
                "%(status_code)s"
            ),
            "use_colors": True,
        },
        "default": {
            "()": "uvicorn.logging.DefaultFormatter",
            "datefmt": "%Y-%m-%dT%H:%M:%S",
            "format": (
                "[%(correlation_id)s%(trace_id_suffix)s] %(levelprefix)s %(message)s"
            ),
            "use_colors": True,
        },
    },
    "handlers": {
        "access": {
            "class": "logging.StreamHandler",
            "filters": ["principal", "correlation_id", "trace_id"],
            "formatter": "access",
            "stream": "ext://sys.stdout",
        },
        "default": {
            "class": "logging.StreamHandler",
            "filters": ["correlation_id", "trace_id"],
            "formatter": "default",
            "stream": "ext://sys.stderr",
        },
    },
    "loggers": {
        "tiled": {
            "handlers": ["default"],
            "level": "INFO",
            "propagate": False,
        },
        "uvicorn.access": {"handlers": ["access"], "level": "INFO", "propagate": False},
        "uvicorn.error": {
            "handlers": ["default"],
            "level": "INFO",
            "propagate": False,
        },
    },
    "version": 1,
}
