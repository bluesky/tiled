# There is no public API in httpx to injecting additional decoders.
from httpx._decoders import SUPPORTED_DECODERS

from ..utils import modules_available

if modules_available("blosc"):

    class BloscDecoder:
        def __init__(self):
            # Blosc seems to have no streaming interface.
            # Accumulate response data in a cache here,
            # and concatenate and decode at the end.
            self._data = []

        def decode(self, data: bytes) -> bytes:
            self._data.append(data)

        def flush(self) -> bytes:
            # Hide this here to defer the numpy import that it triggers.
            import blosc

            if len(self._data) == 1:
                (data,) = self._data
            else:
                data = b"".join(self._data)
            return blosc.decompress(data)

    SUPPORTED_DECODERS["blosc"] = BloscDecoder


if modules_available("zstandard"):
    import zstandard

    class ZStandardDecoder:
        def __init__(self):
            self.context = zstandard.ZstdDecompressor()
            self.decompressobj = self.context.decompressobj()

        def decode(self, data: bytes) -> bytes:
            return self.decompressobj.decompress(data)

        def flush(self) -> bytes:
            return b""

    SUPPORTED_DECODERS["zstd"] = ZStandardDecoder
