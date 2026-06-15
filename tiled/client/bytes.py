import builtins
import itertools
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional, Union

from ..ndslice import NDSlice, split_1d
from .base import BaseClient
from .utils import export_util, handle_error, retry_context

# Chunk size used when streaming a single Range response to disk. Small enough
# to keep peak per-worker memory bounded, large enough to amortize syscalls.
_STREAM_BLOCK_SIZE = 1 * 1024 * 1024  # 1 MiB


def _format_slice(s: slice) -> str:
    """Render a Python slice as the numpy-style string the server expects."""
    parts = [
        "" if s.start is None else str(s.start),
        "" if s.stop is None else str(s.stop),
    ]
    if s.step is not None:
        parts.append(str(s.step))
    return ":".join(parts)


class BytesClient(BaseClient):
    """Client for a `bytes` node: an opaque sequence of bytes."""

    # Upper bound on the expected response body for a single HTTP request.
    # Used to partition large reads/exports into multiple `Range:` requests so
    # no individual request trips the server's `response_bytesize_limit`
    # (default 300 MB) and so memory stays bounded per worker.
    RESPONSE_BYTESIZE_LIMIT = 100 * 1024 * 1024  # 100 MiB

    def __repr__(self):
        return f"<{type(self).__name__} size={self.structure().size}>"

    def __len__(self):
        return self.structure().size

    def _normalize(self, s: Optional[builtins.slice]) -> Optional[tuple[int, int]]:
        """Map a unit-step slice to absolute `(lo, hi)`; `None` for step != 1.

        Returning `None` signals the caller to fall back to a single
        sequential `?slice=` request (reversed or strided reads). Uses
        `NDSlice.expand_for_shape` to normalize `slice(None)`, missing
        bounds, and negative indices into concrete absolute integers.
        """
        size = len(self)
        s = builtins.slice(None) if s is None else s
        abs_s = NDSlice((s,)).expand_for_shape((size,))[0]
        if (abs_s.step or 1) != 1:
            return None
        return (abs_s.start, max(abs_s.stop, abs_s.start))

    def _partition(self, lo: int, hi: int) -> list[tuple[int, int]]:
        """Split `[lo, hi)` into request-sized pieces, preferring chunk boundaries.

        Delegates to `ndslice.split_1d`, which only subdivides when a piece
        exceeds `RESPONSE_BYTESIZE_LIMIT` and otherwise issues a single
        request (the server reassembles assets internally).
        """
        if lo >= hi:
            return [(lo, hi)]
        # Cumulative chunk start offsets are the natural split candidates.
        chunk_starts = list(itertools.accumulate(self.structure().chunks, initial=0))
        return split_1d(
            lo,
            hi,
            1,
            max_len=self.RESPONSE_BYTESIZE_LIMIT,
            pref_splits=chunk_starts,
        )

    def _fetch_range(self, lo: int, hi: int) -> bytes:
        """Fetch `[lo, hi)` via a single `Range:` request; throttled."""
        if lo >= hi:
            return b""
        url = self.item["links"]["full"]
        headers = {"Range": f"bytes={lo}-{hi - 1}"}
        with self.context.throttle():
            for attempt in retry_context(self.context):
                with attempt:
                    response = handle_error(
                        self.context.http_client.get(url, headers=headers)
                    )
        if (ps := self.context.progress_state) is not None:
            ps.advance()
        return response.content

    def _stream_range_to_file(
        self, path: Path, src_lo: int, src_hi: int, dst_off: int
    ) -> None:
        """Stream source bytes `[src_lo, src_hi)` into `path` at `dst_off`.

        Uses `httpx.stream` + `iter_bytes` so peak per-worker memory is bounded
        by `_STREAM_BLOCK_SIZE` rather than the full sub-range size. Each
        worker opens its own file descriptor for thread-safe positional writes.
        Throttled by the context's connection semaphore.
        """
        if src_lo >= src_hi:
            return
        url = self.item["links"]["full"]
        headers = {"Range": f"bytes={src_lo}-{src_hi - 1}"}
        http = self.context.http_client
        with self.context.throttle():
            for attempt in retry_context(self.context):
                with attempt:
                    with http.stream("GET", url, headers=headers) as response:
                        handle_error(response)
                        with open(path, "r+b") as f:
                            f.seek(dst_off)
                            for block in response.iter_bytes(_STREAM_BLOCK_SIZE):
                                f.write(block)
        if (ps := self.context.progress_state) is not None:
            ps.advance()

    def read(self, slice: Optional[builtins.slice] = None) -> bytes:
        """Return the payload, or a slice of it, as `bytes`.

        Parameters
        ----------
        slice : slice or None
            Python slice (e.g. `slice(0, 100)`) or `None` for the full
            payload. For integer indexing, use `client[i]`.

        Notes
        -----
        Unit-step slices are partitioned (preferring chunk boundaries, capped
        at `RESPONSE_BYTESIZE_LIMIT`) and fetched in parallel via HTTP
        `Range:` headers, throttled by the context's `max_connections`.
        Reversed or strided slices fall back to a single sequential
        `?slice=` request.
        """
        rng = self._normalize(slice)
        if rng is None:
            # step != 1: sequential ?slice= fallback.
            params = {"slice": _format_slice(slice)}
            url = self.item["links"]["full"]
            for attempt in retry_context(self.context):
                with attempt:
                    response = handle_error(
                        self.context.http_client.get(url, params=params)
                    )
            return response.content
        lo, hi = rng
        if lo >= hi:
            return b""
        ranges = self._partition(lo, hi)
        if len(ranges) == 1:
            return self._fetch_range(*ranges[0])
        workers = min(len(ranges), self.context.max_connections)
        with self.context.tracking_progress(total=len(ranges)):
            with ThreadPoolExecutor(max_workers=workers) as pool:
                parts = list(pool.map(lambda r: self._fetch_range(*r), ranges))
        return b"".join(parts)

    def __getitem__(self, key: Union[builtins.slice, int]) -> Union[bytes, int]:
        """Match standard `bytes` indexing: int -> int, slice -> bytes."""
        if isinstance(key, int):
            # Normalize negative indices against length so the one-byte slice
            # we request is non-empty (slice(-1, 0) would be empty).
            size = len(self)
            i = key if key >= 0 else key + size
            if not 0 <= i < size:
                raise IndexError("bytes index out of range")
            return self.read(builtins.slice(i, i + 1))[0]
        return self.read(slice=key)

    def export(
        self,
        filepath,
        *,
        format=None,
        slice: Optional[builtins.slice] = None,
    ):
        """Download the payload (or a slice of it) to a file or buffer.

        Parameters
        ----------
        filepath : str, Path, or writeable buffer
            Destination. Paths are written via streaming `Range:` requests
            partitioned across the context's connection pool; buffer
            destinations fall back to a single sequential request.
        format : str, optional
            Format hint, passed through to the server (unused for bytes;
            retained for API symmetry).
        slice : slice or None
            Optional unit-step slice to export. Reversed or strided slices
            fall back to a single sequential `?slice=` request.

        Notes
        -----
        Concurrency is governed by the context's `max_connections` (default
        16); no per-call worker knob is exposed, matching `ArrayClient`. Each
        worker streams its sub-range to disk so peak memory stays bounded
        regardless of payload size.
        """
        rng = self._normalize(slice)
        # Buffer destinations: fall back to single-shot export_util.
        # Strided/reversed slices: same fallback, server handles it.
        if rng is None or not isinstance(filepath, (str, Path)):
            params = {} if slice is None else {"slice": _format_slice(slice)}
            return export_util(
                filepath,
                format,
                self.context.http_client.get,
                self.item["links"]["full"],
                params=params,
            )
        return self._streaming_export(Path(filepath), rng)

    def _streaming_export(self, path: Path, byte_range: tuple[int, int]) -> None:
        lo, hi = byte_range
        size = hi - lo
        # Preallocate so workers can seek+write into disjoint regions.
        with open(path, "wb") as f:
            if size:
                f.truncate(size)
        if size == 0:
            return
        # Partition output offsets are absolute; rebase to [0, size) per worker.
        ranges = self._partition(lo, hi)
        workers = min(len(ranges), self.context.max_connections)

        def fetch(byte_range):
            src_lo, src_hi = byte_range
            self._stream_range_to_file(path, src_lo, src_hi, src_lo - lo)

        with self.context.tracking_progress(total=len(ranges)):
            with ThreadPoolExecutor(max_workers=workers) as pool:
                for _ in pool.map(fetch, ranges):
                    pass
        # Sanity check: streamed bytes should fill the preallocated file.
        actual = os.path.getsize(path)
        if actual != size:
            raise IOError(f"Streaming export wrote {actual} bytes; expected {size}")
