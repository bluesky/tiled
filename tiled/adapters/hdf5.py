import collections.abc
import os
import sys
import warnings
from pathlib import Path
from typing import Any, Iterator, List, Optional, Tuple, Union

import h5py
import numpy
from numpy._typing import NDArray

from ..adapters.utils import IndexersMixin
from ..iterviews import ItemsView, KeysView, ValuesView
from ..structures.array import ArrayStructure
from ..structures.core import Spec, StructureFamily
from ..structures.table import TableStructure
from ..utils import node_repr, path_from_uri
from .array import ArrayAdapter
from .protocols import AccessPolicy
from .resource_cache import with_resource_cache
from .type_alliases import JSON

SWMR_DEFAULT = bool(int(os.getenv("TILED_HDF5_SWMR_DEFAULT", "0")))
INLINED_DEPTH = int(os.getenv("TILED_HDF5_INLINED_CONTENTS_MAX_DEPTH", "7"))


def from_dataset(dataset: NDArray[Any]) -> ArrayAdapter:
    """

    Parameters
    ----------
    dataset :

    Returns
    -------

    """
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
        node: Any,
        *,
        structure: Optional[ArrayStructure] = None,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
        access_policy: Optional[AccessPolicy] = None,
    ) -> None:
        """

        Parameters
        ----------
        node :
        structure :
        metadata :
        specs :
        access_policy :
        """
        self._node = node
        self._access_policy = access_policy
        self.specs = specs or []
        self._provided_metadata = metadata or {}
        super().__init__()

    @classmethod
    def from_file(
        cls,
        file: Any,
        *,
        structure: Optional[TableStructure] = None,
        metadata: Optional[JSON] = None,
        swmr: bool = SWMR_DEFAULT,
        libver: str = "latest",
        specs: Optional[List[Spec]] = None,
        access_policy: Optional[AccessPolicy] = None,
    ) -> "HDF5Adapter":
        """

        Parameters
        ----------
        file :
        structure :
        metadata :
        swmr :
        libver :
        specs :
        access_policy :

        Returns
        -------

        """
        return cls(file, metadata=metadata, specs=specs, access_policy=access_policy)

    @classmethod
    def from_uri(
        cls,
        data_uri: str,
        *,
        structure: Optional[ArrayStructure] = None,
        metadata: Optional[JSON] = None,
        swmr: bool = SWMR_DEFAULT,
        libver: str = "latest",
        specs: Optional[List[Spec]] = None,
        access_policy: Optional[AccessPolicy] = None,
    ) -> "HDF5Adapter":
        """

        Parameters
        ----------
        data_uri :
        structure :
        metadata :
        swmr :
        libver :
        specs :
        access_policy :

        Returns
        -------

        """
        filepath = path_from_uri(data_uri)
        cache_key = (h5py.File, filepath, "r", swmr, libver)
        file = with_resource_cache(
            cache_key, h5py.File, filepath, "r", swmr=swmr, libver=libver
        )
        return cls.from_file(file)

    def __repr__(self) -> str:
        """

        Returns
        -------

        """
        return node_repr(self, list(self))

    @property
    def access_policy(self) -> Optional[AccessPolicy]:
        """

        Returns
        -------

        """
        return self._access_policy

    def structure(self) -> None:
        """

        Returns
        -------

        """
        return None

    def metadata(self) -> JSON:
        """

        Returns
        -------

        """
        d = dict(self._node.attrs)
        for k, v in list(d.items()):
            # Convert any bytes to str.
            if isinstance(v, bytes):
                d[k] = v.decode()
        d.update(self._provided_metadata)
        return d

    def __iter__(self) -> Iterator[Any]:
        """

        Returns
        -------

        """
        yield from self._node

    def __getitem__(self, key: str) -> Union["HDF5Adapter", ArrayAdapter]:
        """

        Parameters
        ----------
        key :

        Returns
        -------

        """
        value = self._node[key]
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
                    dataset_names = value.file[self._node.name + "/" + key][...][()]
                    if value.size == 1:
                        arr = numpy.array(dataset_names)
                        return from_dataset(arr)
                return from_dataset(numpy.array([]))
            return from_dataset(value)

    def __len__(self) -> int:
        """

        Returns
        -------

        """
        return len(self._node)

    def keys(self) -> KeysView:  # type: ignore
        """

        Returns
        -------

        """
        return KeysView(lambda: len(self), self._keys_slice)

    def values(self) -> ValuesView:  # type: ignore
        """

        Returns
        -------

        """
        return ValuesView(lambda: len(self), self._items_slice)

    def items(self) -> ItemsView:  # type: ignore
        """

        Returns
        -------

        """
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
        keys = list(self._node)
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
        """

        Parameters
        ----------
        depth :

        Returns
        -------

        """
        return depth <= INLINED_DEPTH


def hdf5_lookup(
    data_uri: str,
    *,
    structure: Optional[ArrayStructure] = None,
    metadata: Optional[JSON] = None,
    swmr: bool = SWMR_DEFAULT,
    libver: str = "latest",
    specs: Optional[List[Spec]] = None,
    access_policy: Optional[AccessPolicy] = None,
    dataset: Optional[Union[List[Path], List[str]]] = None,
    path: Optional[Union[List[Path], List[str]]] = None,
) -> Union[HDF5Adapter, ArrayAdapter]:
    """

    Parameters
    ----------
    data_uri :
    structure :
    metadata :
    swmr :
    libver :
    specs :
    access_policy :
    dataset :
    path :

    Returns
    -------

    """

    if dataset is not None and path is not None:
        raise ValueError("dataset and path kwargs should not both be set!")

    dataset = dataset or path or []
    adapter = HDF5Adapter.from_uri(
        data_uri,
        structure=structure,
        metadata=metadata,
        swmr=swmr,
        libver=libver,
        specs=specs,
        access_policy=access_policy,
    )
    for segment in dataset:
        adapter = adapter.get(segment)  # type: ignore
        if adapter is None:
            raise KeyError(segment)
    # TODO What to do with metadata, specs?
    return adapter
