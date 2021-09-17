import dask.array
import numpy

from ..structures.xarray import (
    ArrayStructure,
    DataArrayMacroStructure,
    DataArrayStructure,
    DatasetMacroStructure,
    VariableMacroStructure,
    VariableStructure,
)
from ..readers.array import ArrayAdapter
from ..utils import DictView


class VariableAdapter:
    """
    Wrap an xarray.Variable
    """

    structure_family = "variable"

    def __init__(self, variable, metadata=None):
        self._variable = variable
        self._metadata = metadata or {}

    def __repr__(self):
        return f"{type(self).__name__}({self._variable!r})"

    @property
    def metadata(self):
        return DictView(self._metadata)

    def macrostructure(self):
        # Coordinates are greedily loaded into numpy, so we cannot insist or
        # assume that these are dask-backed the way that we do in the other
        # adapters.
        if isinstance(self._variable.data, dask.array.Array):
            array_reader = ArrayAdapter(self._variable.data)
        else:
            array_reader = ArrayAdapter.from_array(self._variable.data)
        return VariableMacroStructure(
            dims=self._variable.dims,
            data=ArrayStructure(
                macro=array_reader.macrostructure(),
                micro=array_reader.microstructure(),
            ),
            attrs=self._variable.attrs,
        )

    def microstructure(self):
        return None

    def read(self):
        return self._variable

    def read_block(self, block, slice=None):
        data = self._variable.data
        if isinstance(data, numpy.ndarray):
            if block != (0,):
                raise NotImplementedError
            return data
        dask_array = data.blocks[block]
        if slice is not None:
            dask_array = dask_array[slice]
        return dask_array.compute()


class DataArrayAdapter:
    """
    Wrap an xarray.DataArray
    """

    structure_family = "data_array"

    def __init__(self, data_array, metadata=None):
        self._data_array = data_array
        self._metadata = metadata or {}

    def __repr__(self):
        return f"{type(self).__name__}({self._data_array!r})"

    @property
    def metadata(self):
        return DictView(self._metadata)

    def macrostructure(self):
        return self._recursive_macrostructure()

    def _recursive_macrostructure(self, depth=0):
        variable_reader = VariableAdapter(self._data_array.variable)
        variable_structure = VariableStructure(
            macro=variable_reader.macrostructure(), micro=None
        )
        coords = {}
        if depth == 0:
            for k, v in self._data_array.coords.items():
                coord_reader = DataArrayAdapter(v)
                coord_structure = DataArrayStructure(
                    macro=coord_reader._recursive_macrostructure(depth=1 + depth),
                    micro=None,
                )
                coords[k] = coord_structure
        return DataArrayMacroStructure(
            variable=variable_structure,
            coords=coords,
            name=self._data_array.name,
        )

    def microstructure(self):
        return None

    def read(self, slice=None):
        data_array = self._data_array
        if slice is not None:
            data_array = data_array[slice]
        return data_array.compute()

    def read_block(self, block, coord=None, slice=None):
        if coord is None:
            adapter = VariableAdapter(self._data_array.variable)
        else:
            adapter = DataArrayAdapter(self._data_array.coords[coord])
        return adapter.read_block(block, slice=slice)


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
            data_vars=data_vars,
            coords=coords,
            attrs=self._dataset.attrs,
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
