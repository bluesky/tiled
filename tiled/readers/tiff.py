import tifffile

from ..structures.array import (
    ArrayMacroStructure,
    MachineDataType,
)


class TiffReader:
    structure_family = "array"

    def __init__(self, path):
        self._file = tifffile.TiffFile(path)

    @property
    def metadata(self):
        # This contains some enums, but Python's built-in JSON serializer
        # handles them fine (converting  to str or int as appropriate).
        return {tag.name: tag.value for tag in self._file.pages[0].tags.values()}

    def read(self, slice=None):
        # TODO Is there support for reading less than the whole array
        # if we only want a slice? I do not think that is possible with a
        # single-page TIFF but I'm not sure. Certainly it *is* possible for
        # multi-page TIFFs.
        arr = self._file.asarray()
        if slice is not None:
            arr = arr[slice]
        return arr

    def read_block(self, block, slice=None):
        if block != (0, 0):
            raise IndexError(block)
        arr = self._file.asarray()
        if slice is not None:
            arr = arr[slice]
        return arr

    def microstructure(self):
        return MachineDataType.from_numpy_dtype(self._file.series[0].dtype)

    def macrostructure(self):
        shape = tuple(self._file.shaped_metadata[0]["shape"])
        return ArrayMacroStructure(
            shape=shape,
            chunks=tuple((dim,) for dim in shape),
        )
