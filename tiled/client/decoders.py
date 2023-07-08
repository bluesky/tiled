# There is no public API in httpx to injecting additional decoders.
from httpx._decoders import SUPPORTED_DECODERS

from ..utils import modules_available

if modules_available("blosc"):

    class BloscDecoder:
        def decode(self, data: bytes) -> bytes:
            # Hide this here to defer the numpy import that it triggers.
            import blosc

            return blosc.decompress(data)

        def flush(self) -> bytes:
            return b""

    SUPPORTED_DECODERS["blosc"] = BloscDecoder


if modules_available("zstandard"):

    import zstandard

    class ZStandardDecoder:
        def __init__(self):
            self.decompressor = zstandard.ZstdDecompressor()

        def decode(self, data: bytes) -> bytes:
            return self.decompressor.decompress(data)

        def flush(self) -> bytes:
            return b""

    SUPPORTED_DECODERS["zstd"] = ZStandardDecoder
