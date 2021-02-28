import itertools

import dask.array
import numpy

from ..query_registration import DictView
from ..sources.array import ArrayStructure


class ClientArraySource:
    def __init__(self, client, metadata, path, container_dispatch):
        self._client = client
        self._metadata = metadata
        self._path = path

    @property
    def metadata(self):
        "Metadata about this Catalog."
        # Ensure this is immutable (at the top level) to help the user avoid
        # getting the wrong impression that editing this would update anything
        # persistent.
        return DictView(self._metadata)

    def describe(self):
        response = self._client.get(
            f"/metadata/{'/'.join(self._path)}", params={"fields": "structure"}
        )
        response.raise_for_status()
        result = response.json()["data"]["attributes"]["structure"]
        return ArrayStructure(**result)

    def _get_block(self, block, dtype, shape):
        """
        Fetch the data for one block in a chunked (dask) array.
        """
        response = self._client.get(
            f"/blob/array/{'/'.join(self._path)}",
            headers={"Accept": "application/octet-stream"},
            params={"block": ",".join(map(str, block))},
        )
        response.raise_for_status()
        return numpy.frombuffer(response.content, dtype=dtype).reshape(shape)

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
