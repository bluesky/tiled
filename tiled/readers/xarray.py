import numpy

from ..containers.xarray import (
    ArrayStructure,
    DataArrayMacroStructure,
    DataArrayStructure,
    DatasetMacroStructure,
    VariableMacroStructure,
    VariableStructure,
)
from ..readers.array import ArrayReader
from ..utils import DictView


class VariableReader:
    """
    Wrap an xarray.Variable
    """

    container = "variable"

    def __init__(self, variable, metadata=None):
        self._variable = variable
        self._metadata = metadata or {}

    def __repr__(self):
        return f"{type(self).__name__}({self._variable!r})"

    @property
    def metadata(self):
        return DictView(self._metadata)

    def macrostructure(self):
        array_reader = ArrayReader(self._variable.data)
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

    def close(self):
        self._variable = None

    def __enter__(self):
        return self

    def __exit__(self):
        self.close()


class DataArrayReader:
    """
    Wrap an xarray.DataArray
    """

    container = "data_array"

    def __init__(self, data_array, metadata=None):
        self._data_array = data_array
        self._metadata = metadata or {}

    def __repr__(self):
        return f"{type(self).__name__}({self._data_array!r})"

    @property
    def metadata(self):
        return DictView(self._metadata)

    def macrostructure(self):
        variable_reader = VariableReader(self._data_array.variable)
        variable_structure = VariableStructure(
            macro=variable_reader.macrostructure(), micro=None
        )
        coords = {}
        for k, v in self._data_array.coords.items():
            coord_reader = VariableReader(v)
            coord_structure = VariableStructure(
                macro=coord_reader.macrostructure(), micro=None
            )
            coords[k] = coord_structure
        return DataArrayMacroStructure(
            variable=variable_structure,
            coords=coords,
            name=self._data_array.name,
        )

    def microstructure(self):
        return None

    def read(self):
        return self._data_array

    def read_block(self, block, coord=None, slice=None):
        if coord is None:
            variable = VariableReader(self._data_array.variable)
        else:
            variable = VariableReader(self._data_array.coords[coord])
        return variable.read_block(block, slice=slice)

    def close(self):
        self._data_array = None

    def __enter__(self):
        return self

    def __exit__(self):
        self.close()


class DatasetReader:
    """
    Wrap an xarray.Dataset
    """

    container = "dataset"

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
            data_array_reader = DataArrayReader(v)
            data_array_structure = DataArrayStructure(
                macro=data_array_reader.macrostructure(), micro=None
            )
            data_vars[k] = data_array_structure
        coords = {}
        for k, v in self._dataset.coords.items():
            coord_reader = VariableReader(v)
            coord_structure = VariableStructure(
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
        return ds

    def read_variable(self, variable):
        return self._dataset[variable]

    def read_block(self, variable, block, coord=None, slice=None):
        if variable in self._dataset.coords:
            return VariableReader(self._dataset.coords[variable]).read_block(
                block, slice=slice
            )
        else:
            return DataArrayReader(self._dataset[variable]).read_block(
                block, coord, slice=slice
            )

    def close(self):
        self._dataset = None

    def __enter__(self):
        return self

    def __exit__(self):
        self.close()
