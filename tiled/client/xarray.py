import xarray

from ..containers.xarray import (
    DataArrayMacroStructure,
    DatasetMacroStructure,
    VariableMacroStructure,
    DataArrayStructure,
    DatasetStructure,
    VariableStructure,
)

from .array import ClientDaskArrayReader
from .base import BaseArrayClientReader


class ClientDaskVariableReader(BaseArrayClientReader):

    MACROSTRUCTURE_TYPE = VariableMacroStructure
    MICROSTRUCTURE_TYPE = None
    STRUCTURE_TYPE = VariableStructure

    def __init__(self, *args, route="/variable/block", **kwargs):
        super().__init__(*args, **kwargs)
        self._route = route

    def read(self):
        structure = self.structure().macro
        array_source = ClientDaskArrayReader(
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


class ClientVariableReader(ClientDaskVariableReader):
    def read(self):
        return super().read().load()


class ClientDaskDataArrayReader(BaseArrayClientReader):

    MACROSTRUCTURE_TYPE = DataArrayMacroStructure
    MICROSTRUCTURE_TYPE = None
    STRUCTURE_TYPE = DataArrayStructure

    def __init__(self, *args, route="/data_array/block", **kwargs):
        super().__init__(*args, **kwargs)
        self._route = route

    def read(self):
        structure = self.structure().macro
        variable = structure.variable
        variable_source = ClientDaskVariableReader(
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
            variable_source = ClientDaskVariableReader(
                client=self._client,
                path=self._path,
                metadata=self.metadata,
                params={"coord": name, **self._params},
                structure=variable,
                route=self._route,
            )
            coords[name] = variable_source.read()
        return xarray.DataArray(data=data, coords=coords, name=structure.name)


class ClientDataArrayReader(ClientDaskDataArrayReader):
    def read(self):
        return super().read().load()


class ClientDaskDatasetReader(BaseArrayClientReader):

    MACROSTRUCTURE_TYPE = DatasetMacroStructure
    MICROSTRUCTURE_TYPE = None
    STRUCTURE_TYPE = DatasetStructure

    def __init__(self, *args, route="/dataset/block", **kwargs):
        super().__init__(*args, **kwargs)
        self._route = route

    def read(self):
        structure = self.structure().macro
        data_vars = {}
        for name, data_array in structure.data_vars.items():
            data_array_source = ClientDaskDataArrayReader(
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
            variable_source = ClientDaskVariableReader(
                client=self._client,
                path=self._path,
                metadata=self.metadata,
                params={"variable": name, **self._params},
                structure=variable,
                route=self._route,
            )
            coords[name] = variable_source.read()
        return xarray.Dataset(data_vars=data_vars, coords=coords, attrs=structure.attrs)


class ClientDatasetReader(ClientDaskDatasetReader):
    def read(self):
        return super().read().load()
