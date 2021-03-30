import collections.abc
import warnings

import dask.array
import h5py
import numpy

from ..readers.array import ArrayAdapter
from ..utils import DictView
from .utils import catalog_repr, IndexersMixin
from ..queries import KeyLookup
from .in_memory import Catalog as CatalogInMemory


class HDF5DatasetAdapter(ArrayAdapter):
    # TODO Just wrap h5py.Dataset directly, not via dask.array.
    def __init__(self, dataset):
        super().__init__(dask.array.from_array(dataset))


class Catalog(collections.abc.Mapping, IndexersMixin):
    def __init__(self, node, access_policy=None, authenticated_identity=None):
        self._node = node
        if (access_policy is not None) and (
            not access_policy.check_compatibility(self)
        ):
            raise ValueError(
                f"Access policy {access_policy} is not compatible with this Catalog."
            )
        self._access_policy = access_policy
        self._authenticated_identity = authenticated_identity
        super().__init__()

    def __repr__(self):
        return catalog_repr(self, list(self))

    @property
    def access_policy(self):
        return self._access_policy

    @property
    def authenticated_identity(self):
        return self._authenticated_identity

    def authenticated_as(self, identity):
        if self._authenticated_identity is not None:
            raise RuntimeError(
                f"Already authenticated as {self.authenticated_identity}"
            )
        if self._access_policy is not None:
            raise NotImplementedError
        else:
            catalog = type(self)(
                self._node,
                access_policy=self._access_policy,
                authenticated_identity=identity,
            )
        return catalog

    @property
    def metadata(self):
        d = dict(self._node.attrs)
        for k, v in list(d.items()):
            # Convert any bytes to str.
            if isinstance(v, bytes):
                d[k] = v.decode()
        return DictView(d)

    def __iter__(self):
        yield from self._node

    def __getitem__(self, key):
        value = self._node[key]
        if isinstance(value, h5py.Group):
            return Catalog(value)
        else:
            if value.dtype == numpy.dtype("O"):
                warnings.warn(
                    f"The dataset {key} is of object type, using a "
                    "Python-only feature of h5py that is not supported by "
                    "HDF5 in general. Read more about that feature at "
                    "https://docs.h5py.org/en/stable/special.html. "
                    "Consider using a fixed-length field instead. "
                    "Tiled will serve an empty placeholder."
                )
                return HDF5DatasetAdapter(numpy.array([]))
            return HDF5DatasetAdapter(value)

    def __len__(self):
        return len(self._node)

    def search(self, query):
        """
        Return a Catalog with a subset of the mapping.
        """
        if isinstance(query, KeyLookup):
            return CatalogInMemory({query.key: self[query.key]})
        else:
            raise NotImplementedError

    # The following three methods are used by IndexersMixin
    # to define keys_indexer, items_indexer, and values_indexer.

    def _keys_slice(self, start, stop):
        return list(self._node)[start:stop]

    def _items_slice(self, start, stop):
        return [(key, self[key]) for key in list(self)[start:stop]]

    def _item_by_index(self, index):
        return self[list(self)[index]]
