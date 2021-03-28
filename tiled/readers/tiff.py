from ..readers.array import (
    ArrayReader,
)


class TiffReader(ArrayReader):
    def __init__(self, path):
        import tifffile

        # TODO Defer the actual reading to later.
        super().__init__(tifffile.imread(path))
