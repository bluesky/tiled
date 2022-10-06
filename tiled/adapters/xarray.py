import collections.abc
import itertools

import xarray

from ..adapters.array import ArrayAdapter
from ..adapters.mapping import MapAdapter


class DatasetAdapter(MapAdapter):
    """
    Wrap an xarray.Dataset
    """

    @classmethod
    def from_dataset(cls, dataset, *, specs=None, references=None):
        mapping = _DatasetMap(dataset)
        return cls(
            mapping,
            metadata={"attrs": dataset.attrs},
            specs=specs,
            references=references,
        )

    def __init__(self, mapping, *args, specs=None, references=None, **kwargs):
        if isinstance(mapping, xarray.Dataset):
            raise TypeError(
                "Use DatasetAdapter.from_dataset(...), not DatasetAdapter(...)."
            )
        specs = specs or []
        references = references or []
        specs.append("xarray_dataset")
        super().__init__(mapping, *args, specs=specs, references=references, **kwargs)

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
            spec = "xarray_coord"
        else:
            spec = "xarray_data_var"
        return ArrayAdapter(
            data_array.data,
            metadata={"attrs": data_array.attrs},
            dims=data_array.dims,
            specs=[spec],
        )
