import builtins
import collections.abc
import os
import sys
import warnings
from pathlib import Path
from typing import Any, Iterator, List, Optional, Tuple, Union

import dask
import dask.array
import dask.delayed
import h5py
import numpy
from numpy._typing import NDArray

from ..adapters.utils import IndexersMixin
from ..catalog.orm import Node
from ..iterviews import ItemsView, KeysView, ValuesView
from ..structures.array import ArrayStructure
from ..structures.core import Spec, StructureFamily
from ..structures.data_source import DataSource
from ..type_aliases import JSON
from ..utils import node_repr, path_from_uri
from .array import ArrayAdapter
from .resource_cache import with_resource_cache

SWMR_DEFAULT = bool(int(os.getenv("TILED_HDF5_SWMR_DEFAULT", "0")))
INLINED_DEPTH = int(os.getenv("TILED_HDF5_INLINED_CONTENTS_MAX_DEPTH", "7"))


def from_dataset(dataset: NDArray[Any]) -> ArrayAdapter:
    return ArrayAdapter.from_array(dataset, metadata=getattr(dataset, "attrs", {}))


if sys.version_info < (3, 9):
    from typing_extensions import Mapping

    MappingType = Mapping
else:
    import collections

    MappingType = collections.abc.Mapping


class HDF5Adapter(MappingType[str, Union["HDF5Adapter", ArrayAdapter]], IndexersMixin):
    """
    Read an HDF5 file or a group within one.

    This map the structure of an HDF5 file onto a "Tree" of array structures.

    Examples
    --------

    From the root node of a file given a filepath

    >>> import h5py
    >>> HDF5Adapter.from_uri("file://localhost/path/to/file.h5")

    From the root node of a file given an h5py.File object

    >>> import h5py
    >>> file = h5py.File("path/to/file.h5")
    >>> HDF5Adapter.from_file(file)

    From a group within a file

    >>> import h5py
    >>> file = h5py.File("path/to/file.h5")
    >>> HDF5Adapter(file["some_group']["some_sub_group"])

    """

    structure_family = StructureFamily.container

    def __init__(
        self,
        file: Any,
        *,
        structure: Optional[ArrayStructure] = None,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
    ) -> None:
        self._file = file
        self.specs = specs or []
        self._metadata = metadata or {}

    @classmethod
    def from_catalog(
        cls,
        data_source: DataSource,
        node: Node,
        /,
        swmr: bool = SWMR_DEFAULT,
        libver: str = "latest",
        **kwargs: Optional[Any],
    ) -> "HDF5Adapter":
        assets = data_source.assets
        if len(assets) == 1:
            data_uri = assets[0].data_uri
        else:
            for ast in assets:
                if ast.parameter == "data_uri":
                    data_uri = ast.data_uri
                    break
        filepath = path_from_uri(data_uri)
        cache_key = (h5py.File, filepath, "r", swmr, libver)
        file = with_resource_cache(
            cache_key, h5py.File, filepath, "r", swmr=swmr, libver=libver
        )

        adapter = cls(
            file,
            structure=data_source.structure,
            metadata=node.metadata_,
            specs=node.specs,
        )
        dataset = kwargs.get("dataset") or kwargs.get("path") or []
        for segment in dataset:
            adapter = adapter.get(segment)  # type: ignore
            if adapter is None:
                raise KeyError(segment)

        return adapter

    @classmethod
    def from_uris(
        cls,
        data_uri: str,
        *,
        swmr: bool = SWMR_DEFAULT,
        libver: str = "latest",
    ) -> "HDF5Adapter":
        filepath = path_from_uri(data_uri)
        cache_key = (h5py.File, filepath, "r", swmr, libver)
        file = with_resource_cache(
            cache_key, h5py.File, filepath, "r", swmr=swmr, libver=libver
        )
        return cls(file)

    def __repr__(self) -> str:
        return node_repr(self, list(self))

    def structure(self) -> None:
        return None

    def metadata(self) -> JSON:
        d = dict(self._file.attrs)
        for k, v in list(d.items()):
            # Convert any bytes to str.
            if isinstance(v, bytes):
                d[k] = v.decode()
        d.update(self._metadata)
        return d

    def __iter__(self) -> Iterator[Any]:
        yield from self._file

    def __getitem__(self, key: str) -> Union["HDF5Adapter", ArrayAdapter]:
        value = self._file[key]
        if isinstance(value, h5py.Group):
            return HDF5Adapter(value)
        else:
            if value.dtype == numpy.dtype("O"):
                warnings.warn(
                    f"The dataset {key} is of object type, using a "
                    "Python-only feature of h5py that is not supported by "
                    "HDF5 in general. Read more about that feature at "
                    "https://docs.h5py.org/en/stable/special.html. "
                    "Consider using a fixed-length field instead. "
                    "Tiled will serve an empty placeholder, unless the "
                    "object is of size 1, where it will attempt to repackage "
                    "the data into a numpy array."
                )

                check_str_dtype = h5py.check_string_dtype(value.dtype)
                if check_str_dtype.length is None:
                    dataset_names = value.file[self._file.name + "/" + key][...][()]
                    if value.size == 1:
                        arr = numpy.array(dataset_names)
                        return from_dataset(arr)
                return from_dataset(numpy.array([]))
            return from_dataset(value)

    def __len__(self) -> int:
        return len(self._file)

    def keys(self) -> KeysView:  # type: ignore
        return KeysView(lambda: len(self), self._keys_slice)

    def values(self) -> ValuesView:  # type: ignore
        return ValuesView(lambda: len(self), self._items_slice)

    def items(self) -> ItemsView:  # type: ignore
        return ItemsView(lambda: len(self), self._items_slice)

    def search(self, query: Any) -> None:
        """

        Parameters
        ----------
        query :

        Returns
        -------
                Return a Tree with a subset of the mapping.

        """
        raise NotImplementedError

    def read(self, fields: Optional[str] = None) -> "HDF5Adapter":
        """

        Parameters
        ----------
        fields :

        Returns
        -------

        """
        if fields is not None:
            raise NotImplementedError
        return self

    # The following two methods are used by keys(), values(), items().

    def _keys_slice(self, start: int, stop: int, direction: int) -> List[Any]:
        """

        Parameters
        ----------
        start :
        stop :
        direction :

        Returns
        -------

        """
        keys = list(self._file)
        if direction < 0:
            keys = list(reversed(keys))
        return keys[start:stop]

    def _items_slice(
        self, start: int, stop: int, direction: int
    ) -> List[Tuple[Any, Any]]:
        """

        Parameters
        ----------
        start :
        stop :
        direction :

        Returns
        -------

        """
        items = [(key, self[key]) for key in list(self)]
        if direction < 0:
            items = list(reversed(items))
        return items[start:stop]

    def inlined_contents_enabled(self, depth: int) -> bool:
        return depth <= INLINED_DEPTH


class HDF5ArrayAdapter(ArrayAdapter):
    """Adapter for array-type data stored in HDF5 files"""

    @staticmethod
    def lazy_load_hdf5_array(
        *file_paths: Union[str, Path],
        dataset: Optional[str] = None,
        swmr: bool = SWMR_DEFAULT,
        libver: str = "latest",
    ) -> dask.array.Array:
        """Lazily load arrays from possibly many HDF5 files"""

        def _read_hdf5_array(fpath: Union[str, Path]) -> NDArray[Any]:
            f = h5py.File(fpath, "r", swmr=swmr, libver=libver)
            return f[dataset] if dataset else f

        def _get_hdf5_specs(
            fpath: Union[str, Path]
        ) -> Tuple[Tuple[int, ...], numpy.dtype]:
            with h5py.File(fpath, "r", swmr=swmr, libver=libver) as f:
                f = f[dataset] if dataset else f
                return f.shape, f.dtype

        # Need to know shapes/dtyeps of constituent arrays to load them lazily
        shapes_dtypes = [_get_hdf5_specs(fpath) for fpath in file_paths]
        delayed = [dask.delayed(_read_hdf5_array)(fpath) for fpath in file_paths]
        arrs = [
            dask.array.from_delayed(val, shape=shape, dtype=dtype)
            for (val, (shape, dtype)) in zip(delayed, shapes_dtypes)
        ]
        array = dask.array.concatenate(arrs, axis=0)

        return array

    @classmethod
    def from_catalog(
        cls,
        data_source: DataSource,
        node: Node,
        /,
        dataset: Optional[str] = None,
        slice: Optional[Tuple[Union[int, builtins.slice], ...]] = None,
        swmr: bool = SWMR_DEFAULT,
        libver: str = "latest",
        **kwargs: Optional[Any],
    ) -> "HDF5ArrayAdapter":
        """Adapter for array data stored in HDF5 files

        Parameters
        ----------
        data_source :
        node :
        kwargs : dict
        """

        structure = data_source.structure
        file_paths = [path_from_uri(ast.data_uri) for ast in data_source.assets]

        array = cls.lazy_load_hdf5_array(
            *file_paths, dataset=dataset, swmr=swmr, libver=libver
        )

        if slice:
            array = array[slice]

        if array.shape != structure.shape:
            raise ValueError(
                f"Shape mismatch between array data and structure: "
                f"{array.shape} != {structure.shape}"
            )
        if array.dtype != structure.dtype:
            raise ValueError(
                f"Data type mismatch between array data and structure: "
                f"{array.dtype} != {structure.dtype}"
            )

        # TODO: Possibly rechunk according to structure.chunks? Is it expensive/necessary?

        return cls(
            array,
            structure,
            metadata=node.metadata_,
            specs=node.specs,
        )

    @classmethod
    def from_uris(
        cls,
        *data_uris: str,
        dataset: Optional[str] = None,
        slice: Optional[Tuple[Union[int, builtins.slice], ...]] = None,
        swmr: bool = SWMR_DEFAULT,
        libver: str = "latest",
        **kwargs: Optional[Any],
    ) -> "HDF5ArrayAdapter":
        file_paths = [path_from_uri(uri) for uri in data_uris]
        array = cls.lazy_load_hdf5_array(
            *file_paths, dataset=dataset, swmr=swmr, libver=libver
        )

        if slice:
            array = array[slice]

        structure = ArrayStructure.from_array(array)

        return cls(array, structure)
