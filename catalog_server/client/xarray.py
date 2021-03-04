import xarray

from ..containers.xarray import VariableStructure
from ..media_type_registration import deserialization_registry
from .array import ClientArraySource
from .utils import BaseClientSource


class ClientVariableSource(BaseClientSource):

    STRUCTURE_TYPE = VariableStructure

    def _get_block(self, block, dtype, shape):
        """
        Fetch the data for one block in a chunked (dask) array.
        """
        media_type = "application/octet-stream"
        response = self._client.get(
            f"/blob/array/{'/'.join(self._path)}",
            headers={"Accept": media_type},
            params={"block": ",".join(map(str, block)), **self._params},
        )
        response.raise_for_status()
        return deserialization_registry(
            "array", media_type, response.content, dtype, shape
        )

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
