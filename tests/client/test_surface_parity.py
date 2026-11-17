"""The ledger of deliberate surface divergences between the sync and async clients."""

import pytest

# --- Surface parity between the sync and async twins -------------------------

# Deliberate divergences of the first async version. If one of these gains an
# async counterpart, remove it here.
CUT_FROM_ASYNC = {
    "login",  # API-key auth only
    "logout",
    "metadata_revisions",
    "parent",
    "export",  # exporting (and progress bars)
    "raw_export",
    "subscribe",  # websockets
    "register",  # file-registration tooling; revisit if needed
}
# Not yet implemented in the async client. Implementing one of these (the
# plan is to implement them all) must remove it here.
PENDING_IN_ASYNC = {
    "array": set(),
    "container": set(),
    "dataframe": set(),
    "sparse": set(),
    "awkward": set(),
    "ragged": set(),
    "bytes": set(),
}
SYNC_ONLY = {
    # dask: the sync non-dask ArrayClient inherits (and overrides with eager
    # versions) the dask client's surface. The public block methods exist to
    # support dask, so the async clients omit them; chunked uploads use the
    # private `_write_block` internally.
    "array": {"compute", "read_block", "write_block"},
    # The async container is not a Mapping: no indexer views; the entry-point
    # registry is module-level (aio.dispatch), not class-level; and the
    # deprecated `write_dataframe` alias is deliberately not carried over
    # (use `write_table`).
    "container": {
        "STRUCTURE_CLIENTS_FROM_ENTRYPOINTS",
        "discover_clients_from_entrypoints",
        "items_indexer",
        "keys_indexer",
        "values_indexer",
        "write_dataframe",
    },
    "dataframe": set(),
    "sparse": {"read_block", "write_block"},
    "awkward": set(),
    "ragged": {"write_block"},
    "bytes": set(),
}
ASYNC_ONLY = {
    "array": set(),
    # Explicit coroutines replacing the Mapping dunders.
    "container": {"len", "lookup"},
    "dataframe": set(),
    "sparse": set(),
    "awkward": set(),
    "ragged": set(),
    "bytes": set(),
}


def public_surface(cls):
    return {name for name in dir(cls) if not name.startswith("_")}


@pytest.mark.parametrize(
    "family",
    ["array", "container", "dataframe", "sparse", "awkward", "ragged", "bytes"],
)
def test_surface_parity(family):
    """The async twins track the sync surface, minus documented divergences.

    This test is the ledger of those divergences: adding a method to one
    twin (or implementing a pending async counterpart) must update it.
    """
    import importlib

    sync_class_names = {
        "array": "ArrayClient",
        "container": "Container",
        "dataframe": "DataFrameClient",
        "sparse": "SparseClient",
        "awkward": "AwkwardClient",
        "ragged": "RaggedClient",
        "bytes": "BytesClient",
    }
    sync_cls = getattr(
        importlib.import_module(f"tiled.client.{family}"), sync_class_names[family]
    )
    async_cls = getattr(
        importlib.import_module(f"tiled.client.aio.{family}"),
        f"Async{sync_class_names[family]}",
    )
    sync_surface = public_surface(sync_cls)
    async_surface = public_surface(async_cls)
    expected_sync_only = (CUT_FROM_ASYNC | PENDING_IN_ASYNC[family]) & sync_surface
    expected_sync_only |= SYNC_ONLY[family]
    assert sync_surface - async_surface == expected_sync_only
    assert async_surface - sync_surface == ASYNC_ONLY[family]
