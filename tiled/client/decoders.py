from typing import Iterator

# There is no public API in httpx2 to injecting additional decoders.
from httpx2._decoders import SUPPORTED_DECODERS

from ..utils import modules_available

if modules_available("blosc2"):

    class Blosc2Decoder:
        def __init__(self):
            # Blosc seems to have no streaming interface.
            # Accumulate response data in a cache here,
            # and concatenate and decode at the end.
            self._data = []

        def decode(self, data: bytes) -> Iterator[bytes]:
            self._data.append(data)
            return ()

        def flush(self) -> Iterator[bytes]:
            # Hide this here to defer the numpy import that it triggers.
            import blosc2

            data = self._data[0] if len(self._data) == 1 else b"".join(self._data)
            # A streaming response may arrive as several concatenated blosc2
            # frames (the server emits one frame per write() call). Since
            # blosc2.decompress reads only the first frame, walk the buffer
            # frame by frame -- each frame header reports its own compressed
            # length (cbytes) -- and concatenate the decompressed pieces.
            view = memoryview(data)
            offset = 0
            while offset < len(view):
                _, cbytes, _ = blosc2.get_cbuffer_sizes(view[offset:])
                if cbytes <= 0:
                    break
                frame = view[offset : offset + cbytes]  # noqa: E203
                yield blosc2.decompress(frame)
                offset += cbytes

    SUPPORTED_DECODERS["blosc2"] = Blosc2Decoder


if modules_available("zstandard"):
    import zstandard

    class ZStandardDecoder:
        def __init__(self):
            self._context = zstandard.ZstdDecompressor()
            self._obj = self._context.decompressobj()

        def decode(self, data: bytes) -> Iterator[bytes]:
            return (self._obj.decompress(data),)

        def flush(self) -> Iterator[bytes]:
            return ()

    SUPPORTED_DECODERS["zstd"] = ZStandardDecoder
