"""Adapter for the opaque-bytes structure family.

A `bytes` node is a logically flat byte stream physically backed by one or
more chunks that concatenate in order. The adapter holds one "chunk reader"
callable per chunk; readers are invoked with `(offset_within_chunk, length)`
and return `bytes`. This keeps the adapter agnostic to the underlying byte
source (in-memory buffer, local file, S3/Azure/GCS blob, etc.).

Slicing into the stream maps to partial reads of the chunks that intersect
the requested range; chunks outside the range are never touched.
"""

from collections.abc import Set
from typing import Any, Callable, Optional, cast
from urllib.parse import urlparse

from ..catalog.orm import Node
from ..storage import (
    SUPPORTED_OBJECT_URI_SCHEMES,
    FileStorage,
    ObjectStorage,
    Storage,
    get_storage,
)
from ..structures.bytes import BytesStructure
from ..structures.core import Spec, StructureFamily
from ..structures.data_source import DataSource
from ..type_aliases import JSON
from ..utils import path_from_uri
from .core import Adapter

# A bytes chunk reader takes (offset_within_chunk, length) and returns that slice
# of the chunk as bytes. `length` is always non-negative and bounded by the
# chunk's declared size.
BytesReader = Callable[[int, int], bytes]


def _buffer_reader(buffer: bytes) -> BytesReader:
    """Build a bytes chunk reader backed by an in-memory bytes-like object."""
    view = memoryview(buffer)

    def read(offset: int, length: int) -> bytes:
        return bytes(view[offset : offset + length])  # noqa: E203

    return read


def _file_reader(data_uri: str) -> BytesReader:
    """Build a bytes chunk reader backed by a local file URI."""
    path = path_from_uri(data_uri)

    def read(offset: int, length: int) -> bytes:
        with open(path, "rb") as f:
            f.seek(offset)
            return f.read(length)

    return read


def _obstore_handle(data_uri: str) -> tuple[Any, str]:
    """Resolve `data_uri` to an (obstore_handle, path_within_store) pair."""
    storage = cast(ObjectStorage, get_storage(data_uri))
    _, _, path = ObjectStorage.parse_blob_uri(data_uri)
    return storage.get_obstore_location(), path


def _object_reader(data_uri: str) -> BytesReader:
    """Build a bytes chunk reader backed by an object-store URI (S3/Azure/GCS)."""
    store, path = _obstore_handle(data_uri)

    def read(offset: int, length: int) -> bytes:
        if length == 0:
            return b""
        return bytes(store.get_range(path, start=offset, length=length))

    return read


def _reader_for_uri(data_uri: str) -> BytesReader:
    """Dispatch to a backend-appropriate reader based on URI scheme."""
    scheme = urlparse(data_uri).scheme
    if scheme == "file":
        return _file_reader(data_uri)
    if scheme in SUPPORTED_OBJECT_URI_SCHEMES:
        return _object_reader(data_uri)
    raise ValueError(
        f"BytesAdapter cannot read URI with unsupported scheme {scheme!r}: "
        f"{data_uri!r}"
    )


def _size_for_uri(data_uri: str) -> int:
    """Return the byte length of the asset at `data_uri`."""
    scheme = urlparse(data_uri).scheme
    if scheme == "file":
        return path_from_uri(data_uri).stat().st_size
    if scheme in SUPPORTED_OBJECT_URI_SCHEMES:
        store, path = _obstore_handle(data_uri)
        return int(store.head(path)["size"])
    raise ValueError(
        f"BytesAdapter cannot stat URI with unsupported scheme {scheme!r}: "
        f"{data_uri!r}"
    )


class BytesAdapter(Adapter[BytesStructure]):
    """Adapter for an opaque sequence of bytes spread across one or more chunks."""

    structure_family: StructureFamily = StructureFamily.bytes

    def __init__(
        self,
        readers: list[BytesReader],
        structure: BytesStructure,
        *,
        metadata: Optional[JSON] = None,
        specs: Optional[list[Spec]] = None,
    ) -> None:
        """Low-level constructor. Prefer `from_buffers`, `from_uris`, or
        `from_catalog` in most cases.

        Parameters
        ----------
        readers:
            One callable per chunk, in concatenation order. Each reader
            accepts `(offset_within_chunk, length)` and returns bytes.
        structure:
            `BytesStructure` whose `chunks` tuple lines up 1-to-1 with `readers`.
        """
        if len(readers) != len(structure.chunks):
            raise ValueError(
                f"BytesAdapter expects one reader per chunk: got "
                f"{len(readers)} readers but structure.chunks has "
                f"length {len(structure.chunks)}"
            )
        self._readers: list[BytesReader] = list(readers)
        super().__init__(structure=structure, metadata=metadata, specs=specs)

    @classmethod
    def supported_storage(cls) -> Set[type[Storage]]:
        return {FileStorage, ObjectStorage}

    @classmethod
    def from_buffers(
        cls,
        *buffers: bytes,
        metadata: Optional[JSON] = None,
        specs: Optional[list[Spec]] = None,
    ) -> "BytesAdapter":
        """Build an in-memory BytesAdapter from one or more byte buffers.

        Each positional buffer becomes one chunk; argument order defines
        concatenation order. Chunk sizes are inferred from buffer lengths.
        The analog of `ArrayAdapter.from_array`.
        """
        chunks = tuple(len(b) for b in buffers)
        structure = BytesStructure(size=sum(chunks), chunks=chunks)
        readers = [_buffer_reader(b) for b in buffers]
        return cls(readers, structure, metadata=metadata, specs=specs)

    @classmethod
    def from_uris(
        cls,
        *data_uris: str,
        metadata: Optional[JSON] = None,
        specs: Optional[list[Spec]] = None,
    ) -> "BytesAdapter":
        """Build a BytesAdapter from a list of file or object-store URIs.

        Each URI is stat-ed (filesystem `stat` for `file://`, an object-store
        HEAD for blob URIs) and its size becomes the corresponding entry in
        `structure.chunks`. The order of `data_uris` defines concatenation
        order. URIs may mix schemes.
        """
        chunks = tuple(_size_for_uri(uri) for uri in data_uris)
        structure = BytesStructure(size=sum(chunks), chunks=chunks)
        readers = [_reader_for_uri(uri) for uri in data_uris]
        return cls(readers, structure, metadata=metadata, specs=specs)

    @classmethod
    def from_catalog(
        cls,
        data_source: DataSource[BytesStructure],
        node: Node,
        /,
        **kwargs: Optional[Any],
    ) -> "BytesAdapter":
        # Order assets by `num` so that index i in `data_uris` lines up with
        # `structure.chunks[i]`. Assets that were registered without a `num`
        # (shouldn't happen for bytes nodes, but be defensive) are kept in
        # iteration order at the end.
        ordered = sorted(
            data_source.assets,
            key=lambda a: a.num if a.num is not None else float("inf"),
        )
        readers = [_reader_for_uri(a.data_uri) for a in ordered]
        return cls(
            readers,
            structure=data_source.structure,
            metadata=node.metadata_,
            specs=node.specs,
        )

    # The bytes family is read-only by design. To store new data in Tiled,
    # users should pick a structured family (array, table, awkward, ...).
    # `bytes` exists as a fallback for serving externally registered files
    # that have no useful logical structure.

    def read(self, slice: Optional[slice] = None) -> bytes:
        """Return the bytes covered by `slice`, or the entire payload if None.

        Only chunks whose byte ranges intersect the requested slice are read.
        Negative indices, omitted bounds, and non-unit (including negative)
        step are supported via `slice.indices(size)`; semantics match
        `payload[slice]`.
        """
        size = self._structure.size
        if slice is None:
            start, stop, step = 0, size, 1
        else:
            start, stop, step = slice.indices(size)

        # Identify the forward [lo, hi) byte range actually referenced by the
        # slice. For step > 0 this is just [start, stop); for step < 0,
        # `slice.indices` yields start > stop with stop possibly -1, and the
        # touched indices are stop+1 .. start inclusive.
        if step > 0:
            lo, hi = start, stop
        else:
            lo, hi = stop + 1, start + 1
        if lo >= hi:
            return b""

        out = bytearray()
        offset = 0
        for chunk_size, reader in zip(self._structure.chunks, self._readers):
            chunk_end = offset + chunk_size
            if chunk_end <= lo:
                offset = chunk_end
                continue
            if offset >= hi:
                break
            chunk_lo = max(0, lo - offset)
            chunk_hi = min(chunk_size, hi - offset)
            out.extend(reader(chunk_lo, chunk_hi - chunk_lo))
            offset = chunk_end

        if step == 1:
            return bytes(out)
        # Re-index into the forward range we fetched. `out` corresponds to
        # bytes [lo, hi); payload[i] == out[i - lo] for i in [lo, hi).
        return bytes(out[i - lo] for i in range(start, stop, step))
