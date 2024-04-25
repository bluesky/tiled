# There is no public API in httpx to injecting additional decoders.
from httpx._decoders import SUPPORTED_DECODERS

from ..utils import modules_available

if modules_available("blosc2"):

    class Blosc2Decoder:
        def __init__(self):
            # Blosc seems to have no streaming interface.
            # Accumulate response data in a cache here,
            # and concatenate and decode at the end.
            self._data = []

        def decode(self, data: bytes) -> bytes:
            self._data.append(data)
            return b""

        def flush(self) -> bytes:
            # Hide this here to defer the numpy import that it triggers.
            import blosc2

            if len(self._data) == 1:
                (data,) = self._data
            else:
                data = b"".join(self._data)
            return blosc2.decompress(data)

    SUPPORTED_DECODERS["blosc2"] = Blosc2Decoder


if modules_available("zstandard"):
    import zstandard

    class ZStandardDecoder:
        def __init__(self):
            self._context = zstandard.ZstdDecompressor()
            self._obj = self._context.decompressobj()

        def decode(self, data: bytes) -> bytes:
            return self._obj.decompress(data)

        def flush(self) -> bytes:
            return b""

    SUPPORTED_DECODERS["zstd"] = ZStandardDecoder
