import urllib.parse
from copy import copy
from logging import LogRecord

from uvicorn.logging import AccessFormatter as _UvicornAccessFormatter


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
    },
    "formatters": {
        "access": {
            "()": "tiled.server.logging_config.AccessFormatter",
            "datefmt": "%Y-%m-%dT%H:%M:%S",
            "format": (
                "[%(correlation_id)s] "
                '%(client_addr)s (%(principal)s) - "%(request_line)s" '
                "%(status_code)s"
            ),
            "use_colors": True,
        },
        "default": {
            "()": "uvicorn.logging.DefaultFormatter",
            "datefmt": "%Y-%m-%dT%H:%M:%S",
            "format": "[%(correlation_id)s] %(levelprefix)s %(message)s",
            "use_colors": True,
        },
    },
    "handlers": {
        "access": {
            "class": "logging.StreamHandler",
            "filters": ["principal", "correlation_id"],
            "formatter": "access",
            "stream": "ext://sys.stdout",
        },
        "default": {
            "class": "logging.StreamHandler",
            "filters": ["correlation_id"],
            "formatter": "default",
            "stream": "ext://sys.stderr",
        },
    },
    "loggers": {
        "uvicorn.access": {"handlers": ["access"], "level": "INFO", "propagate": False},
        "uvicorn.error": {
            "handlers": ["default"],
            "level": "INFO",
            "propagate": False,
        },
    },
    "version": 1,
}
