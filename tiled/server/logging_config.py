LOGGING_CONFIG = {
    "disable_existing_loggers": False,
    "filters": {
        "correlation_id": {
            "()": "asgi_correlation_id.CorrelationIdFilter",
            "default_value": "-",
            "uuid_length": 16,
        }
    },
    "formatters": {
        "access": {
            "()": "uvicorn.logging.AccessFormatter",
            "datefmt": "%Y-%m-%dT%H:%M:%S",
            "format": "[%(asctime)s.%(msecs)03dZ] "
            "[%(correlation_id)s] %(levelprefix)s "
            '%(client_addr)s - "%(request_line)s" '
            "%(status_code)s",
        },
        "default": {
            "()": "uvicorn.logging.DefaultFormatter",
            "datefmt": "%Y-%m-%dT%H:%M:%S",
            "format": "[%(asctime)s.%(msecs)03dZ] "
            "[%(correlation_id)s] %(levelprefix)s "
            "%(message)s",
            "use_colors": True,
        },
    },
    "handlers": {
        "access": {
            "class": "logging.StreamHandler",
            "filters": ["correlation_id"],
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
