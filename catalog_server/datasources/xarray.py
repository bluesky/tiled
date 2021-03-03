from ..containers.data_array import DataArrayStructure
from ..datasources.array import ArraySource
from ..utils import DictView


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
            dims=self._dims,
            data=ArraySource(self._data_array.data).describe(),
            coords=self._data_array.coords,
            attrs=self._data_array.attrs,
            name=self._data_array.name,
        )

    def read(self):
        return self._data_array
