import xarray

from ..containers.xarray import DataArrayStructure, VariableStructure
from .array import ClientArraySource
from .utils import BaseClientSource


class ClientVariableSource(BaseClientSource):

    STRUCTURE_TYPE = VariableStructure

    def read(self):
        structure = self.describe()
        array_source = ClientArraySource(
            client=self._client,
            path=self._path,
            metadata=self.metadata,
            params=self._params,
            structure=structure.data,
            route="/blob/variable",
        )
        return xarray.Variable(
            dims=structure.dims, data=array_source.read(), attrs=structure.attrs
        )


class ClientDataArraySource(BaseClientSource):

    STRUCTURE_TYPE = DataArrayStructure

    def read(self):
        structure = self.describe()
        # Construct ClientArraySource for the `data` and each of the `coords`.
        variable = structure.variable
        array_source = ClientArraySource(
            client=self._client,
            path=self._path,
            metadata=self.metadata,
            params=self._params,
            structure=variable.data,
            route="/blob/data_array",
        )
        data = xarray.Variable(
            dims=variable.dims, data=array_source.read(), attrs=variable.attrs
        )
        coords = {}
        for name, variable in structure.coords.items():
            array_source = ClientArraySource(
                client=self._client,
                path=self._path,
                metadata=self.metadata,
                params={"coord": name, **self._params},
                structure=variable.data,
                route="/blob/data_array",
            )
            coords[name] = xarray.Variable(
                dims=variable.dims, data=array_source.read(), attrs=variable.attrs
            )
        return xarray.DataArray(data=data, coords=coords, name=structure.name)
