"""EXPERIMENTAL: the default registry of asynchronous structure clients.

Dispatch itself is shared with the synchronous client
(`tiled.client.utils.client_for_item`): an item's specs are checked first
(the most specific signal), then its structure family. Only the registry
differs. Third-party packages may register async structure clients under
the `tiled.structure_client_async` entry-point group, keyed by spec or
structure family name.
"""

from __future__ import annotations

from typing import Optional

from ...structures.core import StructureFamily
from ...utils import OneShotCachedMap

_DEFAULT_STRUCTURE_CLIENTS: Optional[OneShotCachedMap] = None


def default_structure_clients() -> OneShotCachedMap:
    "The default mapping of structure family / spec name to async client class."
    global _DEFAULT_STRUCTURE_CLIENTS
    if _DEFAULT_STRUCTURE_CLIENTS is None:
        import entrypoints

        mapping = {
            StructureFamily.array: lambda: _import("array", "AsyncArrayClient"),
            StructureFamily.awkward: lambda: _import("awkward", "AsyncAwkwardClient"),
            StructureFamily.bytes: lambda: _import("bytes", "AsyncBytesClient"),
            StructureFamily.container: lambda: _import("container", "AsyncContainer"),
            StructureFamily.ragged: lambda: _import("ragged", "AsyncRaggedClient"),
            StructureFamily.sparse: lambda: _import("sparse", "AsyncSparseClient"),
            StructureFamily.table: lambda: _import("dataframe", "AsyncDataFrameClient"),
        }
        for name, entrypoint in entrypoints.get_group_named(
            "tiled.structure_client_async"
        ).items():
            mapping[name] = entrypoint.load
        _DEFAULT_STRUCTURE_CLIENTS = OneShotCachedMap(mapping)
    return _DEFAULT_STRUCTURE_CLIENTS


def _import(module_name: str, class_name: str):
    import importlib

    module = importlib.import_module(f"..{module_name}", __name__)
    return getattr(module, class_name)
