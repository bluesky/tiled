import itertools

import dask.array

from ..containers.array import ArrayStructure, Endianness, Kind, MachineDataType
from ..media_type_registration import deserialization_registry
from ..utils import DictView


class ClientArraySource:
    def __init__(self, client, *, path, metadata, container_dispatch, params):
        self._client = client
        self._metadata = metadata
        self._path = path
        self._params = params

    def __repr__(self):
        return f"<{type(self).__name__}>"

    @property
    def metadata(self):
        "Metadata about this Catalog."
        # Ensure this is immutable (at the top level) to help the user avoid
        # getting the wrong impression that editing this would update anything
        # persistent.
        return DictView(self._metadata)

    def describe(self):
        response = self._client.get(
            f"/metadata/{'/'.join(self._path)}",
            params={"fields": "structure", **self._params},
        )
        response.raise_for_status()
        result = response.json()["data"]["attributes"]["structure"]
        return ArrayStructure(
            chunks=tuple(map(tuple, result["chunks"])),
            shape=tuple(result["shape"]),
            dtype=MachineDataType(
                kind=Kind(result["dtype"]["kind"]),
                itemsize=result["dtype"]["itemsize"],
                endianness=Endianness(result["dtype"]["endianness"]),
            ),
        )

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
        shape = structure.shape
        dtype = structure.dtype.to_numpy_dtype()
        # Build a client-side dask array whose chunks pull from a server-side
        # dask array.
        name = "remote-dask-array-{self._client.base_url!s}{'/'.join(self._path)}"
        chunks = structure.chunks
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
