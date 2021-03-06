import xarray

from ..containers.xarray import DataArrayStructure, VariableStructure
from .array import ClientArraySource
from .base import BaseClientSource


class ClientVariableSource(BaseClientSource):

    STRUCTURE_TYPE = VariableStructure

    def __init__(self, *args, route="/blob/variable", **kwargs):
        super().__init__(*args, **kwargs)
        self._route = route

    def read(self):
        structure = self.describe()
        array_source = ClientArraySource(
            client=self._client,
            path=self._path,
            metadata=self.metadata,
            params=self._params,
            structure=structure.data,
            route=self._route,
        )
        return xarray.Variable(
            dims=structure.dims, data=array_source.read(), attrs=structure.attrs
        )


class ClientDataArraySource(BaseClientSource):

    STRUCTURE_TYPE = DataArrayStructure

    def __init__(self, *args, route="/blob/data_array", **kwargs):
        super().__init__(*args, **kwargs)
        self._route = route

    def read(self):
        structure = self.describe()
        # Construct ClientArraySource for the `data` and each of the `coords`.
        variable = structure.variable
        variable_source = ClientVariableSource(
            client=self._client,
            path=self._path,
            metadata=self.metadata,
            params=self._params,
            structure=variable,
            route=self._route,
        )
        data = variable_source.read()
        coords = {}
        for name, variable in structure.coords.items():
            variable_source = ClientVariableSource(
                client=self._client,
                path=self._path,
                metadata=self.metadata,
                params={"coord": name, **self._params},
                structure=variable,
                route=self._route,
            )
            coords[name] = variable_source.read()
        return xarray.DataArray(data=data, coords=coords, name=structure.name)
