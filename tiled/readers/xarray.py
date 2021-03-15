import numpy

from ..containers.xarray import DataArrayStructure, DatasetStructure, VariableStructure
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

    def structure(self):
        return VariableStructure(
            dims=self._variable.dims,
            data=ArrayReader(self._variable.data).structure(),
            attrs=self._variable.attrs,
        )

    def read(self):
        return self._variable

    def read_block(self, block):
        data = self._variable.data
        if isinstance(data, numpy.ndarray):
            if block != (0,):
                raise NotImplementedError
            return data
        return data.blocks[block].compute()

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

    def structure(self):
        return DataArrayStructure(
            variable=VariableReader(self._data_array.variable).structure(),
            coords={
                k: VariableReader(v).structure()
                for k, v in self._data_array.coords.items()
            },
            name=self._data_array.name,
        )

    def read(self):
        return self._data_array

    def read_block(self, block, coord=None):
        if coord is None:
            variable = VariableReader(self._data_array.variable)
        else:
            variable = VariableReader(self._data_array.coords[coord])
        return variable.read_block(block)

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

    def structure(self):
        return DatasetStructure(
            data_vars={
                key: DataArrayReader(value).structure()
                for key, value in self._dataset.data_vars.items()
            },
            coords={
                key: VariableReader(value).structure()
                for key, value in self._dataset.coords.items()
            },
            attrs=self._dataset.attrs,
        )

    def read(self):
        return self._dataset

    def read_block(self, variable, block, coord=None):
        if variable in self._dataset.coords:
            return VariableReader(self._dataset.coords[variable]).read_block(block)
        else:
            return DataArrayReader(self._dataset[variable]).read_block(block, coord)

    def close(self):
        self._dataset = None

    def __enter__(self):
        return self

    def __exit__(self):
        self.close()
