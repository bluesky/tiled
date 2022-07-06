import collections.abc

import dask.array
import xarray

from ..adapters.array import ArrayAdapter
from ..adapters.mapping import MapAdapter


class DatasetAdapter(MapAdapter):
    """
    Wrap an xarray.Dataset
    """

    @classmethod
    def from_dataset(cls, dataset, *, specs=None):
        mapping = _DatasetMap(dataset)
        specs = specs or []
        specs.append("xarray_dataset")
        return cls(mapping, metadata=dataset.attrs, specs=specs)

    def __init__(self, mapping, *args, **kwargs):
        if isinstance(mapping, xarray.Dataset):
            raise TypeError(
                "Use DatasetAdapter.from_dataset(...), not DatasetAdapter(...)."
            )
        super().__init__(mapping, *args, **kwargs)

    def as_dataset(self):
        # We do not stash the original dataset as state.
        # We (re)construct one here, ensure that any filtering that was done
        # is respected.
        data_vars = {}
        coords = {}
        for key, array_adapter in self.items():
            if "xarray_data_var" in array_adapter.specs:
                data_vars[key] = (
                    array_adapter.macrostructure().dims,
                    array_adapter.read(),
                )
            elif "xarray_coord" in array_adapter.specs:
                coords[key] = (
                    array_adapter.macrostructure().dims,
                    array_adapter.read(),
                )
            else:
                assert False, "Expected a spec"
        return xarray.Dataset(data_vars=data_vars, coords=coords, attrs=self.metadata)


class _DatasetMap(collections.abc.Mapping):
    def __init__(self, dataset):
        self._dataset = dataset

    def __len__(self):
        return len(self._dataset.data_vars) + len(self._dataset.coords)

    def __iter__(self):
        yield from (list(self._dataset.data_vars) + list(self._dataset.coords))

    def __getitem__(self, key):
        data_array = self._dataset[key]
        if key in self._dataset.coords:
            spec = "xarray_coord"
        else:
            spec = "xarray_data_var"
        if isinstance(data_array.data, dask.array.Array):
            func = ArrayAdapter
        else:
            func = ArrayAdapter.from_array
        return func(
            data_array.data,
            metadata=data_array.attrs,
            dims=data_array.dims,
            specs=[spec],
        )
