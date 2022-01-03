import sys

import dask.array

from tiled.structures.array import ArrayStructure

from ..adapters.array import ArrayAdapter
from ..structures.xarray import (
    DataArrayMacroStructure,
    DataArrayStructure,
    DatasetMacroStructure,
)
from ..trees.in_memory import Tree

if sys.version_info < (3, 8):
    from cached_property import cached_property  # isort:skip
else:
    from functools import cached_property  # isort:skip


class VariableAdapter:
    """
    Wrap an xarray.Variable
    """

    structure_family = "array"
    specs = ["variable"]

    def __init__(self, variable):
        self._variable = variable

    def __repr__(self):
        return f"{type(self).__name__}({self._variable!r})"

    @cached_property
    def metadata(self):
        return self._variable.attrs

    @cached_property
    def _array_adapter(self):
        # Coordinates are greedily loaded into numpy, so we cannot insist or
        # assume that these are dask-backed the way that we do in the other
        # adapters.
        if isinstance(self._variable.data, dask.array.Array):
            array_adapter = ArrayAdapter(self._variable.data, dims=self._variable.dims)
        else:
            array_adapter = ArrayAdapter.from_array(
                self._variable.data, dims=self._variable.dims
            )
        return array_adapter

    def macrostructure(self):
        return self._array_adapter.macrostructure()

    def microstructure(self):
        return self._array_adapter.microstructure()

    def structure(self):
        return ArrayStructure(macro=self.macrostructure(), micro=self.microstructure())

    def read(self, *args, **kwargs):
        return self._array_adapter.read(*args, **kwargs)

    def read_block(self, *args, **kwargs):
        return self._array_adapter.read_block(*args, **kwargs)


class DataArrayAdapter:
    """
    Wrap an xarray.DataArray
    """

    structure_family = "xarray_data_array"

    @classmethod
    def from_data_array(cls, data_array, _depth=0):
        variable = VariableAdapter(data_array.variable)
        if _depth == 0:
            coords = Tree(
                {
                    name: cls.from_data_array(coord, _depth=1 + _depth)
                    for name, coord in data_array.coords.items()
                }
            )
        else:
            coords = {}
        return cls(variable, coords, data_array.name)

    def __init__(self, variable, coords, name):
        self._variable = variable
        self._coords = coords
        self._name = name

    @property
    def metadata(self):
        return {
            "variable": self._variable.metadata,
            "coords": {name: c.metadata for name, c in self._coords.items()},
        }

    def __repr__(self):
        return f"<{type(self).__name__}>"

    def __getitem__(self, key):
        if key == "variable":
            return self._variable
        elif key == "coords":
            return self._coords
        else:
            raise KeyError(key)

    def microstructure(self):
        return None

    def macrostructure(self):
        return DataArrayMacroStructure(
            variable=self._variable.structure(),
            coords={k: v.structure() for k, v in self._coords.items()},
            name=self._name,
        )

    def structure(self):
        return DataArrayStructure(macro=self.macrostructure(), micro=None)

    def read(self):
        raise NotImplementedError
        # return xarray.DataArray(...)


class DatasetAdapter:
    """
    Wrap an xarray.Dataset
    """

    structure_family = "xarray_dataset"

    def __init__(self, dataset):
        self._dataset = dataset
        self._data_vars = Tree(
            {
                k: DataArrayAdapter.from_data_array(v)
                for k, v in self._dataset.data_vars.items()
            }
        )
        self._coords = Tree(
            {
                k: DataArrayAdapter.from_data_array(v)
                for k, v in self._dataset.coords.items()
            }
        )

    @property
    def metadata(self):
        return {
            "attrs": self._dataset.attrs,
            "data_vars": {name: da.metadata for name, da in self._data_vars.items()},
            "coords": {name: da.metadata for name, da in self._coords.items()},
        }

    def microstructure(self):
        return None

    def macrostructure(self):
        data_vars = {}
        for k, v in self._dataset.data_vars.items():
            adapter = DataArrayAdapter.from_data_array(v)
            data_vars[k] = {
                "macro": adapter.macrostructure(),
                "micro": adapter.microstructure(),
            }
        coords = {}
        for k, v in self._dataset.coords.items():
            adapter = DataArrayAdapter.from_data_array(v)
            coords[k] = {
                "macro": adapter.macrostructure(),
                "micro": adapter.microstructure(),
            }
        return DatasetMacroStructure(data_vars=data_vars, coords=coords)

    def __repr__(self):
        return f"<{type(self).__name__}>"

    def read(self, fields=None):
        ds = self._dataset
        if fields is not None:
            ds = ds[fields]
        return ds.compute()

    def __getitem__(self, key):
        if key == "data_vars":
            return self._data_vars
        elif key == "coords":
            return self._coords
        else:
            raise KeyError(key)
