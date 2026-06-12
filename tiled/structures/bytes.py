from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Tuple

from tiled.structures.root import Structure


@dataclass
class BytesStructure(Structure):
    """Structure describing an opaque sequence of bytes.

    A `bytes` node is a logically flat byte stream of length `size`,
    physically backed by one or more chunks that concatenate in order.
    Each entry in `chunks` is the byte length of the corresponding
    underlying asset. The invariant `size == sum(chunks)` is enforced.

    The interpretation of the payload (MIME type, filename, encoding)
    is delegated to the `DataSource` / `Asset`. Instead, `BytesStructure`
    intentionally describes _only_ the layout that the server needs to
    resolve a byte-range slice into a set of per-chunk reads.
    """

    size: int  # total logical byte length
    chunks: Tuple[int, ...]  # per-chunk byte lengths; sum(chunks) == size

    def __post_init__(self) -> None:
        # Coerce chunks to a tuple of ints (JSON round-trips list of ints).
        object.__setattr__(self, "chunks", tuple(int(c) for c in self.chunks))
        object.__setattr__(self, "size", int(self.size))
        if any(c < 0 for c in self.chunks):
            raise ValueError(
                f"BytesStructure.chunks entries must be non-negative; got {self.chunks!r}"
            )
        if self.size < 0:
            raise ValueError(
                f"BytesStructure.size must be non-negative; got {self.size!r}"
            )
        total_from_chunks = sum(self.chunks)
        if total_from_chunks != self.size:
            raise ValueError(
                "BytesStructure invariant violated: "
                f"size ({self.size}) != sum(chunks) ({total_from_chunks})"
            )

    @classmethod
    def from_json(cls, structure: Mapping[str, Any]) -> "BytesStructure":
        return cls(
            size=structure["size"],
            chunks=tuple(structure["chunks"]),
        )
