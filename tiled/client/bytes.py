import builtins
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional, Union

from .base import BaseClient
from .utils import export_util, handle_error, retry_context


def _format_slice(s: slice) -> str:
    """Render a Python slice as the numpy-style string the server expects."""
    parts = [
        "" if s.start is None else str(s.start),
        "" if s.stop is None else str(s.stop),
    ]
    if s.step is not None:
        parts.append(str(s.step))
    return ":".join(parts)


def _partition(size: int, chunks: tuple, workers: int) -> list[tuple[int, int]]:
    """Pick byte ranges for parallel download.

    Prefer the server's declared chunk boundaries (one task per chunk), so each
    request maps onto a single backend asset read. If there is only one chunk
    but multiple workers are requested, fall back to an even partition.
    """
    if len(chunks) > 1:
        ranges = []
        offset = 0
        for c in chunks:
            ranges.append((offset, offset + c))
            offset += c
        return ranges
    if workers <= 1 or size == 0:
        return [(0, size)]
    step = -(-size // workers)  # ceiling div
    return [(lo, min(lo + step, size)) for lo in range(0, size, step)]


class BytesClient(BaseClient):
    """Client for a `bytes` node: an opaque sequence of bytes."""

    def __repr__(self):
        return f"<{type(self).__name__} size={self.structure().size}>"

    def __len__(self):
        return self.structure().size

    def read(self, slice: Optional[builtins.slice] = None) -> bytes:
        """Return the payload, or a slice of it, as ``bytes``.

        Parameters
        ----------
        slice : slice or None
            Python slice (e.g. ``slice(0, 100)``) or ``None`` for the full
            payload. For integer indexing, use ``client[i]``.
        """
        params = {}
        if slice is not None and slice != builtins.slice(None):
            params["slice"] = _format_slice(slice)
        for attempt in retry_context():
            with attempt:
                response = handle_error(
                    self.context.http_client.get(
                        self.item["links"]["full"], params=params
                    )
                )
        return response.content

    def __getitem__(self, key: Union[builtins.slice, int]) -> Union[bytes, int]:
        """Match standard ``bytes`` indexing: int -> int, slice -> bytes."""
        if isinstance(key, int):
            # Normalize negative indices against length so the one-byte slice
            # we request is non-empty (slice(-1, 0) would be empty).
            size = len(self)
            i = key if key >= 0 else key + size
            if not 0 <= i < size:
                raise IndexError("bytes index out of range")
            return self.read(builtins.slice(i, i + 1))[0]
        return self.read(slice=key)

    def export(self, filepath, *, format=None, workers: int = 1):
        """Download the payload to a file.

        Parameters
        ----------
        filepath : str, Path, or writeable buffer
            Destination. A buffer forces ``workers=1``.
        format : str, optional
            Passed to the server as a format hint (unused for bytes nodes;
            retained for API symmetry with other clients).
        workers : int, default 1
            When > 1 and ``filepath`` is a path, download byte ranges in
            parallel using a thread pool. Each task issues a single HTTP
            request with a ``Range:`` header; ranges are aligned to the
            server-declared chunk boundaries when there is more than one
            chunk. This is bandwidth-bound, so the right value depends on
            the server's per-connection throughput (typically 4-16 for
            object-store backends, 1 for local files).
        """
        if workers <= 1 or not isinstance(filepath, (str, Path)):
            return export_util(
                filepath,
                format,
                self.context.http_client.get,
                self.item["links"]["full"],
                params={},
            )
        return self._parallel_export(Path(filepath), workers)

    def _parallel_export(self, path: Path, workers: int) -> None:
        size = len(self)
        ranges = _partition(size, self.structure().chunks, workers)
        url = self.item["links"]["full"]
        http = self.context.http_client

        # Preallocate so workers can seek+write into disjoint regions.
        with open(path, "wb") as f:
            if size:
                f.truncate(size)

        def fetch(byte_range: tuple[int, int]) -> None:
            lo, hi = byte_range
            if lo >= hi:
                return
            headers = {"Range": f"bytes={lo}-{hi - 1}"}
            for attempt in retry_context():
                with attempt:
                    response = handle_error(http.get(url, headers=headers))
            data = response.content
            # Each worker opens its own file descriptor for thread-safe writes.
            with open(path, "r+b") as f:
                f.seek(lo)
                f.write(data)

        with ThreadPoolExecutor(max_workers=workers) as pool:
            for _ in pool.map(fetch, ranges):
                pass
        # Sanity check: server should have returned exactly `size` bytes total.
        actual = os.path.getsize(path)
        if actual != size:
            raise IOError(
                f"Parallel export wrote {actual} bytes; expected {size}"
            )
