from ..containers.xarray import DataArrayStructure, VariableStructure
from ..datasources.array import ArraySource
from ..utils import DictView


class VariableSource:
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

    def describe(self):
        return VariableStructure(
            dims=self._variable.dims,
            data=ArraySource(self._variable.data).describe(),
            attrs=self._variable.attrs,
        )

    def read(self):
        return self._variable


class DataArraySource:
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

    def describe(self):
        return DataArrayStructure(
            variable=VariableSource(self._data_array.variable).describe(),
            coords={
                k: VariableSource(v).describe()
                for k, v in self._data_array.coords.items()
            },
            name=self._data_array.name,
        )

    def read(self):
        return self._data_array
