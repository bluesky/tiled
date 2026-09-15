import enum


class ScopeName(str, enum.Enum):
    """
    The canonical set of permission scopes.

    ``ScopeName`` compares and hashes equal to the corresponding plain
    string and can be used interchangeably wherever a scope-name string is
    expected -- including in the ``ScopeName``-valued sets derived below and
    the catalog ``scopes`` table's enum column.
    """

    read_metadata = "read:metadata"
    read_data = "read:data"
    write_metadata = "write:metadata"
    write_data = "write:data"
    delete_revision = "delete:revision"
    delete_node = "delete:node"
    create_node = "create:node"
    register = "register"
    metrics = "metrics"
    create_apikeys = "create:apikeys"
    revoke_apikeys = "revoke:apikeys"
    admin_apikeys = "admin:apikeys"
    read_principals = "read:principals"
    write_principals = "write:principals"
    read_webhooks = "read:webhooks"
    write_webhooks = "write:webhooks"


SCOPES = {
    ScopeName.read_metadata: {"description": "Read metadata."},
    ScopeName.read_data: {"description": "Read data."},
    ScopeName.write_metadata: {"description": "Write metadata."},
    ScopeName.write_data: {"description": "Write data."},
    ScopeName.delete_revision: {"description": "Delete metadata revisions."},
    ScopeName.delete_node: {"description": "Delete a node."},
    ScopeName.create_node: {"description": "Add a node."},
    ScopeName.register: {"description": "Register externally-managed assets."},
    ScopeName.metrics: {"description": "Access (Prometheus) metrics."},
    ScopeName.create_apikeys: {
        "description": "Create API keys as the currently-authenticated user or service."
    },
    ScopeName.revoke_apikeys: {
        "description": "Revoke API keys as the currently-authenticated user or service."
    },
    ScopeName.admin_apikeys: {
        "description": "Create and revoke API keys on behalf of any user or service."
    },
    ScopeName.read_principals: {
        "description": "Read list of all users and services and their attributes."
    },
    ScopeName.write_principals: {
        "description": "Edit list of all users and services and their attributes."
    },
    ScopeName.read_webhooks: {"description": "Read webhooks and delivery history."},
    ScopeName.write_webhooks: {"description": "Register and delete webhooks."},
}

ALL_SCOPES: frozenset[ScopeName] = frozenset(SCOPES)
PUBLIC_SCOPES: frozenset[ScopeName] = frozenset(
    (ScopeName.read_metadata, ScopeName.read_data)
)
SINGLE_USER_SCOPES: frozenset[ScopeName] = frozenset(
    (
        ScopeName.read_metadata,
        ScopeName.read_data,
        ScopeName.write_metadata,
        ScopeName.write_data,
        ScopeName.delete_revision,
        ScopeName.delete_node,
        ScopeName.create_node,
        ScopeName.register,
        ScopeName.metrics,
        ScopeName.read_webhooks,
        ScopeName.write_webhooks,
    )
)
NO_SCOPES: frozenset[ScopeName] = frozenset()
