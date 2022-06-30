import builtins
import hashlib

import tifffile

from ..server.object_cache import with_object_cache
from ..structures.array import ArrayMacroStructure, BuiltinDtype


class TiffAdapter:
    """
    Read a TIFF file.

    Examples
    --------

    >>> TiffAdapter("path/to/file.tiff")
    """

    structure_family = "array"

    def __init__(self, path, *, specs=None, references=None):
        self._file = tifffile.TiffFile(path)
        self._cache_key = (type(self).__module__, type(self).__qualname__, path)
        self.specs = specs or []
        self.references = references or []

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
        arr = with_object_cache(self._cache_key, self._file.asarray)
        if slice is not None:
            arr = arr[slice]
        return arr

    def read_block(self, block, slice=None):
        # For simplicity, this adapter always treat a single TIFF file as one
        # chunk. This could be relaxed in the future.
        if sum(block) != 0:
            raise IndexError(block)

        arr = with_object_cache(self._cache_key, self._file.asarray)
        if slice is not None:
            arr = arr[slice]
        return arr

    def microstructure(self):
        return BuiltinDtype.from_numpy_dtype(self._file.series[0].dtype)

    def macrostructure(self):
        if self._file.is_shaped:
            shape = tuple(self._file.shaped_metadata[0]["shape"])
        else:
            arr = with_object_cache(self._cache_key, self._file.asarray)
            shape = arr.shape
        return ArrayMacroStructure(shape=shape, chunks=tuple((dim,) for dim in shape))


class TiffSequenceAdapter:

    structure_family = "array"

    def __init__(self, seq, *, specs=None, references=None):
        self._seq = seq
        self._metadata = {}
        self._cache_key = (
            type(self).__module__,
            type(self).__qualname__,
            hashlib.md5(str(seq.files).encode()).hexdigest(),
        )
        self.specs = specs or []
        self.references = references or []

    @property
    def metadata(self):
        return self._metadata

    def read(self, slice=None):
        """Return a numpy array

        Receives a sequence of values to select from a collection of tiff files that were saved in a folder
        The input order is defined as files --> X slice --> Y slice
        read() can receive one value or one slice to select all the data from one file or a sequence of files;
        or it can receive a tuple of up to three values (int or slice) to select a more specific sequence of pixels
        of a group of images
        """

        # Print("Inside Adapter:", slice)
        if slice is None:
            return with_object_cache(self._cache_key, self._seq.asarray)
        if isinstance(slice, int):
            # e.g. read(slice=0)
            return with_object_cache(
                self._cache_key + (slice,),
                tifffile.TiffFile(self._seq.files[slice]).asarray,
            )
        # e.g. read(slice=(...))
        if isinstance(slice, tuple):
            if len(slice) == 0:
                return with_object_cache(self._cache_key, self._seq.asarray)
            image_axis, *the_rest = slice
            # Could be int or slice
            # (0, slice(...)) or (0,....) are converted to a list
            if isinstance(image_axis, int):
                # e.g. read(slice=(0, ....))
                arr = with_object_cache(
                    self._cache_key + (image_axis,),
                    tifffile.TiffFile(self._seq.files[image_axis]).asarray,
                )
            if isinstance(image_axis, builtins.slice):
                if image_axis.start is None:
                    slice_start = 0
                else:
                    slice_start = image_axis.start
                if image_axis.step is None:
                    slice_step = 1
                else:
                    slice_step = image_axis.step

                arr = with_object_cache(
                    self._cache_key,
                    tifffile.TiffSequence(
                        self._seq.files[
                            slice_start : image_axis.stop : slice_step  # noqa: E203
                        ]
                    ).asarray,
                )
            arr = arr[tuple(the_rest)]
            return arr
        if isinstance(slice, builtins.slice):
            # Check for start and step which can be optional
            if slice.start is None:
                slice_start = 0
            else:
                slice_start = slice.start
            if slice.step is None:
                slice_step = 1
            else:
                slice_step = slice.step

            arr = with_object_cache(
                self._cache_key,
                tifffile.TiffSequence(
                    self._seq.files[slice_start : slice.stop : slice_step]  # noqa: E203
                ).asarray,
            )
            return arr

    def read_block(self, block, slice=None):
        if block[1:] != (0, 0):
            raise IndexError(block)
        arr = self.read(builtins.slice(block[0], block[0] + 1))
        if slice is not None:
            arr = arr[slice]
        return arr

    def microstructure(self):
        # Assume all files have the same data type
        return BuiltinDtype.from_numpy_dtype(self.read(slice=0).dtype)

    def macrostructure(self):
        shape = (len(self._seq), *self.read(slice=0).shape)
        return ArrayMacroStructure(
            shape=shape,
            # one chunks per underlying TIFF file
            chunks=((1,) * shape[0], (shape[1],), (shape[2],)),
        )


def subdirectory_handler(path):
    """
    Sniff a subdirectory for TIFF sequences.
    """
    # Max fraction of files that do not look like TIFF sequence files
    RELATIVE_THRESHOLD = 0.2
    # Max number of files that do not look like TIFF sequence files
    ABSOLUTE_THRESHOLD = 10
    filepaths = []
    outliers = 0
    for filepath in path.iterdir():
        if not filepath.is_file():
            # Skip directories.
            continue
        if filepath.name.startswith("."):
            # Skip hidden files.
            continue
        if (filepath.suffix in (".tif", ".tiff")) and filepath.stem[-3:].isdigit():
            # This looks like something123.tif.
            filepaths.append(filepath)
            continue
        outliers += 1

    fraction = outliers / (outliers + len(filepaths))
    if (outliers <= ABSOLUTE_THRESHOLD) and (fraction <= RELATIVE_THRESHOLD):
        # This looks like a TIFF sequence directory.
        import tifffile

        seq = tifffile.TiffSequence(sorted(filepaths))
        return TiffSequenceAdapter(seq)

    return None
