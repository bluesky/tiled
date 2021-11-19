import sys

import dask.array

from ..adapters.array import ArrayAdapter
from ..structures.xarray import DataArrayStructure, DatasetMacroStructure
from ..trees.in_memory import Tree
from ..utils import DictView

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
        return DictView({"attrs": self._variable.attrs, "dims": self._variable.dims})

    @cached_property
    def _array_adapter(self):
        # Coordinates are greedily loaded into numpy, so we cannot insist or
        # assume that these are dask-backed the way that we do in the other
        # adapters.
        if isinstance(self._variable.data, dask.array.Array):
            array_adapter = ArrayAdapter(self._variable.data)
        else:
            array_adapter = ArrayAdapter.from_array(self._variable.data)
        return array_adapter

    def macrostructure(self):
        return self._array_adapter.macrostructure()

    def microstructure(self):
        return self._array_adapter.microstructure()

    def read(self, *args, **kwargs):
        return self._array_adapter.read(*args, **kwargs)

    def read_block(self, *args, **kwargs):
        return self._array_adapter.read_block(*args, **kwargs)


class DataArrayAdapter(Tree):
    """
    Wrap an xarray.DataArray
    """

    specs = ["data_array"]

    @classmethod
    def from_data_array(cls, data_array, depth=0):
        # TODO recursion with coords that are also DataArrays that may
        # contian themselves
        mapping = {"variable": VariableAdapter(data_array.variable)}
        if depth == 0:
            mapping["coords"] = Tree(
                {
                    name: cls.from_data_array(coord, depth=1 + depth)
                    for name, coord in data_array.coords.items()
                }
            )
        return cls(mapping, metadata={"name": data_array.name})

    def __repr__(self):
        return f"<{type(self).__name__}>"


class DatasetAdapter:
    """
    Wrap an xarray.Dataset
    """

    structure_family = "dataset"

    def __init__(self, dataset, metadata=None):
        self._dataset = dataset
        self._metadata = metadata or {}

    def __repr__(self):
        return f"{type(self).__name__}({self._dataset!r})"

    @property
    def metadata(self):
        return DictView(self._metadata)

    def macrostructure(self):
        data_vars = {}
        for k, v in self._dataset.data_vars.items():
            data_array_reader = DataArrayAdapter(v)
            data_array_structure = DataArrayStructure(
                macro=data_array_reader.macrostructure(), micro=None
            )
            data_vars[k] = data_array_structure
        coords = {}
        for k, v in self._dataset.coords.items():
            coord_reader = DataArrayAdapter(v)
            coord_structure = DataArrayStructure(
                macro=coord_reader.macrostructure(), micro=None
            )
            coords[k] = coord_structure
        return DatasetMacroStructure(
            data_vars=data_vars, coords=coords, attrs=self._dataset.attrs
        )

    def microstructure(self):
        return None

    def read(self, variables=None):
        ds = self._dataset
        if variables is not None:
            ds = ds[variables]
        return ds.compute()

    def read_variable(self, variable):
        return self._dataset[variable].compute()

    def read_block(self, variable, block, coord=None, slice=None):
        if variable is None:
            return DataArrayAdapter(self._dataset.coords[coord]).read_block(
                block, slice=slice
            )
        else:
            return DataArrayAdapter(self._dataset[variable]).read_block(
                block, coord, slice=slice
            )
