from ..containers.xarray import DataArrayStructure, DatasetStructure, VariableStructure
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

    def structure(self):
        return VariableStructure(
            dims=self._variable.dims,
            data=ArraySource(self._variable.data).structure(),
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

    def structure(self):
        return DataArrayStructure(
            variable=VariableSource(self._data_array.variable).structure(),
            coords={
                k: VariableSource(v).structure()
                for k, v in self._data_array.coords.items()
            },
            name=self._data_array.name,
        )

    def read(self):
        return self._data_array


class DatasetSource:
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
                key: DataArraySource(value).structure()
                for key, value in self._dataset.data_vars.items()
            },
            coords={
                key: VariableSource(value).structure()
                for key, value in self._dataset.coords.items()
            },
            attrs=self._dataset.attrs,
        )

    def read(self):
        return self._dataset
