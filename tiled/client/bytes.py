"""Client for the `bytes` structure family.

A `bytes` node has no content endpoint and no specialized read path. The
client exists primarily to (a) identify a node as `bytes` for downstream
dispatch and (b) provide a more informative `__repr__` than `BaseClient`
when data-source information is available.

Use `raw_export(...)` (inherited from `BaseClient`) to download the
underlying assets, either to a directory on disk or into an in-memory
`MutableMapping` of `BytesIO` buffers.
"""

from ..structures.core import StructureFamily
from .base import BaseClient


def _format_size(num_bytes: float) -> str:
    """Render a byte count using binary prefixes (1 KB = 1024 B)."""
    for unit in ("B", "KB", "MB", "GB", "TB", "PB"):
        if num_bytes < 1024 or unit == "PB":
            if unit == "B":
                return f"{int(num_bytes)} B"
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024
    # Unreachable: the loop returns on the last unit.
    return f"{num_bytes:.1f} EB"


class BytesClient(BaseClient):
    """Client for a `bytes` structure-family node.

    `bytes` nodes carry no structural information of their own; downloads go
    through `/api/v1/asset/bytes/{path}?id=N`, one request per underlying
    asset, gated by `settings.expose_raw_assets`. The high-level entry point
    is `raw_export(...)` (inherited from `BaseClient`), which supports both
    directory and in-memory `MutableMapping` destinations.
    """

    structure_family: StructureFamily = StructureFamily.bytes

    def __repr__(self) -> str:
        """Render as `<BytesClient total across N assets>` when known.

        Falls back to the bare class name when no asset information is
        cached on the client (e.g. when the client was not constructed with
        `include_data_sources=True`). This avoids triggering a network
        round-trip inside `__repr__`.
        """
        data_sources = (
            self.item.get("attributes", {}).get("data_sources") or []
        )
        assets = [a for ds in data_sources for a in ds.get("assets", [])]
        if not assets:
            return f"<{type(self).__name__}>"
        plural = "" if len(assets) == 1 else "s"
        sizes = [a.get("size") for a in assets]
        if any(s is None for s in sizes):
            return f"<{type(self).__name__} {len(assets)} asset{plural}>"
        return (
            f"<{type(self).__name__} {_format_size(sum(sizes))} "
            f"across {len(assets)} asset{plural}>"
        )
