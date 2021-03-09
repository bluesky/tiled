import xarray

from ..containers.xarray import DataArrayStructure, DatasetStructure, VariableStructure
from .array import ClientDaskArraySource
from .base import BaseClientSource


class ClientDaskVariableSource(BaseClientSource):

    STRUCTURE_TYPE = VariableStructure

    def __init__(self, *args, route="/blob/variable", **kwargs):
        super().__init__(*args, **kwargs)
        self._route = route

    def read(self):
        structure = self.describe()
        array_source = ClientDaskArraySource(
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


class ClientVariableSource(ClientDaskVariableSource):
    def read(self):
        return super().read().load()


class ClientDaskDataArraySource(BaseClientSource):

    STRUCTURE_TYPE = DataArrayStructure

    def __init__(self, *args, route="/blob/data_array", **kwargs):
        super().__init__(*args, **kwargs)
        self._route = route

    def read(self):
        structure = self.describe()
        variable = structure.variable
        variable_source = ClientDaskVariableSource(
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
            variable_source = ClientDaskVariableSource(
                client=self._client,
                path=self._path,
                metadata=self.metadata,
                params={"coord": name, **self._params},
                structure=variable,
                route=self._route,
            )
            coords[name] = variable_source.read()
        return xarray.DataArray(data=data, coords=coords, name=structure.name)


class ClientDataArraySource(ClientDaskDataArraySource):
    def read(self):
        return super().read().load()


class ClientDaskDatasetSource(BaseClientSource):

    STRUCTURE_TYPE = DatasetStructure

    def __init__(self, *args, route="/blob/dataset", **kwargs):
        super().__init__(*args, **kwargs)
        self._route = route

    def read(self):
        structure = self.describe()
        data_vars = {}
        for name, data_array in structure.data_vars.items():
            data_array_source = ClientDaskDataArraySource(
                client=self._client,
                path=self._path,
                metadata=self.metadata,
                params={"variable": name, **self._params},
                structure=data_array,
                route=self._route,
            )
            data_vars[name] = data_array_source.read()
        coords = {}
        for name, variable in structure.coords.items():
            variable_source = ClientDaskVariableSource(
                client=self._client,
                path=self._path,
                metadata=self.metadata,
                params={"variable": name, **self._params},
                structure=variable,
                route=self._route,
            )
            coords[name] = variable_source.read()
        return xarray.Dataset(data_vars=data_vars, coords=coords, attrs=structure.attrs)


class ClientDatasetSource(ClientDaskDatasetSource):
    def read(self):
        return super().read().load()
