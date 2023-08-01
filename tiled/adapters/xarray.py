import collections.abc
import itertools

import xarray

from ..structures.core import Spec
from .array import ArrayAdapter
from .mapping import MapAdapter


class DatasetAdapter(MapAdapter):
    """
    Wrap an xarray.Dataset
    """

    @classmethod
    def from_dataset(cls, dataset, *, specs=None, access_policy=None):
        mapping = _DatasetMap(dataset)
        return cls(
            mapping,
            metadata={"attrs": dataset.attrs},
            specs=specs,
            access_policy=access_policy,
        )

    def __init__(self, mapping, *args, specs=None, access_policy=None, **kwargs):
        if isinstance(mapping, xarray.Dataset):
            raise TypeError(
                "Use DatasetAdapter.from_dataset(...), not DatasetAdapter(...)."
            )
        specs = specs or []
        specs.append(Spec("xarray_dataset"))
        super().__init__(
            mapping, *args, specs=specs, access_policy=access_policy, **kwargs
        )

    def inlined_contents_enabled(self, depth):
        # Tell the server to in-line the description of each array
        # (i.e. data_vars and coords) to avoid latency of a second
        # request.
        return True


class _DatasetMap(collections.abc.Mapping):
    def __init__(self, dataset):
        self._dataset = dataset

    def __len__(self):
        return len(self._dataset.data_vars) + len(self._dataset.coords)

    def __iter__(self):
        yield from itertools.chain(self._dataset.data_vars, self._dataset.coords)

    def __getitem__(self, key):
        data_array = self._dataset[key]
        if key in self._dataset.coords:
            spec = Spec("xarray_coord")
        else:
            spec = Spec("xarray_data_var")
        return ArrayAdapter.from_array(
            data_array.data,
            metadata={"attrs": data_array.attrs},
            dims=data_array.dims,
            specs=[spec],
        )
