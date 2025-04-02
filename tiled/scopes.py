SCOPES = {
    "read:metadata": {"description": "Read metadata."},
    "read:data": {"description": "Read data."},
    "write:metadata": {"description": "Write metadata."},
    "write:data": {"description": "Write data."},
    "create": {"description": "Add a node."},
    "register": {"description": "Register externally-managed assets."},
    "metrics": {"description": "Access (Prometheus) metrics."},
    "apikeys": {
        "description": "Create and revoke API keys as the currently-authenticated user or service."
    },
    "admin:apikeys": {
        "description": "Create and revoke API keys on behalf of any user or service."
    },
    "read:principals": {
        "description": "Read list of all users and services and their attributes."
    },
    "write:principals": {
        "description": "Edit list of all users and services and their attributes."
    },
}

ALL_SCOPES: set[str] = frozenset(SCOPES)
PUBLIC_SCOPES: set[str] = frozenset(("read:metadata", "read:data"))
USER_SCOPES: set[str] = frozenset(
    (
        "read:metadata",
        "read:data",
        "write:metadata",
        "write:data",
        "create",
        "register",
        "metrics",
    )
)
NO_SCOPES: set[str] = frozenset()
