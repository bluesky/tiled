import itertools

import dask.array

from ..containers.array import ArrayStructure, MachineDataType
from ..media_type_registration import deserialization_registry
from .base import BaseArrayClientReader
from .utils import handle_error


class ClientDaskArrayReader(BaseArrayClientReader):
    "Client-side wrapper around an array-like that returns dask arrays"

    MACROSTRUCTURE_TYPE = ArrayStructure
    MICROSTRUCTURE_TYPE = MachineDataType

    def __init__(self, *args, route="/array/block", **kwargs):
        super().__init__(*args, **kwargs)
        if route.endswith("/"):
            route = route[:-1]
        self._route = route

    def _get_block(self, block, dtype, shape):
        """
        Fetch the data for one block in a chunked (dask) array.
        """
        media_type = "application/octet-stream"
        response = self._client.get(
            self._route + "/" + "/".join(self._path),
            headers={"Accept": media_type},
            params={"block": ",".join(map(str, block)), **self._params},
        )
        handle_error(response)
        return deserialization_registry(
            "array", media_type, response.content, dtype, shape
        )

    def read(self):
        structure = self.structure()
        shape = structure["macro"].shape
        dtype = structure["micro"].to_numpy_dtype()
        # Build a client-side dask array whose chunks pull from a server-side
        # dask array.
        name = (
            "remote-dask-array-"
            f"{self._client.base_url!s}/{'/'.join(self._path)}"
            f"{'-'.join(map(repr, sorted(self._params.items())))}"
        )
        chunks = structure["macro"].chunks
        # Count the number of blocks along each axis.
        num_blocks = (range(len(n)) for n in chunks)
        # Loop over each block index --- e.g. (0, 0), (0, 1), (0, 2) .... ---
        # and build a dask task encoding the method for fetching its data from
        # the server.
        dask_tasks = {
            (name,)
            + block: (
                self._get_block,
                block,
                dtype,
                tuple(chunks[dim][i] for dim, i in enumerate(block)),
            )
            for block in itertools.product(*num_blocks)
        }
        return dask.array.Array(
            dask=dask_tasks,
            name=name,
            chunks=chunks,
            dtype=dtype,
            shape=shape,
        )


class ClientArrayReader(ClientDaskArrayReader):
    "Client-side wrapper around an array-like that returns in-memory arrays"

    def read(self):
        return super().read().compute()
