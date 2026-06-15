import builtins
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

    def export(self, filepath, *, format=None):
        """Download the payload to a file."""
        return export_util(
            filepath,
            format,
            self.context.http_client.get,
            self.item["links"]["full"],
            params={},
        )
