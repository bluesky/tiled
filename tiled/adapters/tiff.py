import builtins

import tifffile

from ..structures.array import ArrayStructure, BuiltinDtype
from ..structures.core import StructureFamily
from ..utils import path_from_uri
from .resource_cache import with_resource_cache


class TiffAdapter:
    """
    Read a TIFF file.

    Examples
    --------

    >>> TiffAdapter("path/to/file.tiff")
    """

    structure_family = StructureFamily.array

    def __init__(
        self,
        data_uri,
        *,
        structure=None,
        metadata=None,
        specs=None,
        access_policy=None,
    ):
        if not isinstance(data_uri, str):
            raise Exception
        filepath = path_from_uri(data_uri)
        cache_key = (tifffile.TiffFile, filepath)
        self._file = with_resource_cache(cache_key, tifffile.TiffFile, filepath)
        self.specs = specs or []
        self._provided_metadata = metadata or {}
        self.access_policy = access_policy
        if structure is None:
            if self._file.is_shaped:
                shape = tuple(self._file.shaped_metadata[0]["shape"])
            else:
                arr = self._file.asarray()
                shape = arr.shape
            structure = ArrayStructure(
                shape=shape,
                chunks=tuple((dim,) for dim in shape),
                data_type=BuiltinDtype.from_numpy_dtype(self._file.series[0].dtype),
            )
        self._structure = structure

    def metadata(self):
        # This contains some enums, but Python's built-in JSON serializer
        # handles them fine (converting  to str or int as appropriate).
        d = {tag.name: tag.value for tag in self._file.pages[0].tags.values()}
        d.update(self._provided_metadata)
        return d

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
        # For simplicity, this adapter always treat a single TIFF file as one
        # chunk. This could be relaxed in the future.
        if sum(block) != 0:
            raise IndexError(block)

        arr = self._file.asarray()
        if slice is not None:
            arr = arr[slice]
        return arr

    def structure(self):
        return self._structure


class TiffSequenceAdapter:
    structure_family = "array"

    @classmethod
    def from_uris(
        cls,
        data_uris,
        structure=None,
        metadata=None,
        specs=None,
        access_policy=None,
    ):
        filepaths = [path_from_uri(data_uri) for data_uri in data_uris]
        seq = tifffile.TiffSequence(filepaths)
        return cls(
            seq,
            structure=structure,
            specs=specs,
            metadata=metadata,
            access_policy=access_policy,
        )

    def __init__(
        self,
        seq,
        *,
        structure=None,
        metadata=None,
        specs=None,
        access_policy=None,
    ):
        self._seq = seq
        # TODO Check shape, chunks against reality.
        self.specs = specs or []
        self._provided_metadata = metadata or {}
        self.access_policy = access_policy
        if structure is None:
            shape = (len(self._seq), *self.read(slice=0).shape)
            structure = ArrayStructure(
                shape=shape,
                # one chunks per underlying TIFF file
                chunks=((1,) * shape[0], *[(i,) for i in shape[1:]]),
                # Assume all files have the same data type
                data_type=BuiltinDtype.from_numpy_dtype(self.read(slice=0).dtype),
            )
        self._structure = structure

    def metadata(self):
        # TODO How to deal with the many headers?
        return self._provided_metadata

    def read(self, slice=None):
        """Return a numpy array

        Receives a sequence of values to select from a collection of tiff files that were saved in a folder
        The input order is defined as: files --> vertical slice --> horizontal slice --> color slice --> ...
        read() can receive one value or one slice to select all the data from one file or a sequence of files;
        or it can receive a tuple (int or slice) to select a more specific sequence of pixels of a group of images.
        """

        if (slice is None) or (slice is Ellipsis):
            return self._seq.asarray()
        if isinstance(slice, int):
            # e.g. read(slice=0) -- return an entire image
            return tifffile.TiffFile(self._seq.files[slice]).asarray()
        if isinstance(slice, builtins.slice):
            # e.g. read(slice=(...)) -- return a slice along the image axis
            return tifffile.TiffSequence(self._seq.files[slice]).asarray()
        if isinstance(slice, tuple):
            if len(slice) == 0:
                return self._seq.asarray()
            if len(slice) == 1:
                return self.read(slice=slice[0])
            image_axis, *the_rest = slice
            # Could be int or slice (0, slice(...)) or (0,....); the_rest is converted to a list
            if isinstance(image_axis, int):
                # e.g. read(slice=(0, ....))
                arr = tifffile.TiffFile(self._seq.files[image_axis]).asarray()
            elif image_axis is Ellipsis:
                # Return all images
                arr = tifffile.TiffSequence(self._seq.files).asarray()
                the_rest.insert(0, Ellipsis)     # Include any leading dimensions
            elif isinstance(image_axis, builtins.slice):
                arr = self.read(slice=image_axis)
            arr = arr[tuple(the_rest)]
            # NOTE: may need to force np.array type if arr is a scalar
            return arr

    def read_block(self, block, slice=None):
        if any(block[1:]):
            # e.g. block[1:] != [0,0, ..., 0]
            raise IndexError(block)
        arr = self.read(builtins.slice(block[0], block[0] + 1))
        if slice is not None:
            arr = arr[slice]
        return arr

    def structure(self):
        return self._structure
