import itertools

import dask
import dask.array

from ..structures.array import ArrayStructure
from ..media_type_registration import deserialization_registry
from .base import BaseArrayClientReader
from .utils import get_content_with_cache


class ClientDaskArrayAdapter(BaseArrayClientReader):
    "Client-side wrapper around an array-like that returns dask arrays"

    STRUCTURE_TYPE = ArrayStructure

    def __init__(self, *args, route="/array/block", **kwargs):
        super().__init__(*args, **kwargs)
        if route.endswith("/"):
            route = route[:-1]
        self._route = route

    def _get_block(self, block, dtype, shape, slice=None):
        """
        Fetch the actual data for one block in a chunked (dask) array.

        See read_block() for a public version of this. This private version
        enables more efficient multi-block access by requiring the caller to
        pass in the structure (dtype, shape).
        """
        media_type = "application/octet-stream"
        if slice is not None:
            # TODO The server accepts a slice parameter but we'll need to write
            # careful code here to determine what the new shape will be.
            raise NotImplementedError(
                "Slicing less than one block is not yet supported."
            )
        content = get_content_with_cache(
            self._cache,
            self._offline,
            self._client,
            self._route + "/" + "/".join(self._path),
            headers={"Accept": media_type},
            params={"block": ",".join(map(str, block)), **self._params},
        )
        return deserialization_registry("array", media_type, content, dtype, shape)

    def read_block(self, block, slice=None):
        """
        Acess the data for one block of this chunked (dask) array.

        Optionally, access only a slice *within* this block.
        """
        structure = self.structure()
        chunks = structure.macro.chunks
        dtype = structure.micro.to_numpy_dtype()
        try:
            shape = tuple(chunks[dim][i] for dim, i in enumerate(block))
        except IndexError:
            raise IndexError(f"Block index {block} out of range")
        dask_array = dask.array.from_delayed(
            dask.delayed(self._get_block)(block, dtype, shape), dtype=dtype, shape=shape
        )
        # TODO Make the request in _get_block include the slice so that we only
        # fetch exactly the data that we want. This will require careful code
        # to determine what the shape will be.
        if slice is not None:
            dask_array = dask_array[slice]
        return dask_array

    def read(self, slice=None):
        """
        Acess the entire array or a slice.

        The array will be internally chunked with dask.
        """
        structure = self.structure()
        shape = structure.macro.shape
        dtype = structure.micro.to_numpy_dtype()
        # Build a client-side dask array whose chunks pull from a server-side
        # dask array.
        name = (
            "remote-dask-array-"
            f"{self._client.base_url!s}/{'/'.join(self._path)}"
            f"{'-'.join(map(repr, sorted(self._params.items())))}"
        )
        chunks = structure.macro.chunks
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
        dask_array = dask.array.Array(
            dask=dask_tasks,
            name=name,
            chunks=chunks,
            dtype=dtype,
            shape=shape,
        )
        if slice is not None:
            dask_array = dask_array[slice]
        return dask_array

    def __getitem__(self, slice):
        return self.read(slice)

    # The default object.__iter__ works as expected here, no need to
    # implemented it specifically.

    def __len__(self):
        # As with numpy, len(arr) is the size of the zeroth axis.
        return self.structure().macro.shape[0]

    def touch(self):
        super().touch()
        self.read().compute()


class ClientArrayAdapter(ClientDaskArrayAdapter):
    "Client-side wrapper around an array-like that returns in-memory arrays"

    def read(self, slice=None):
        return super().read(slice).compute()

    def read_block(self, block, slice=None):
        return super().read_block(block, slice).compute()
