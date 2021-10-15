import builtins
import itertools

import dask
import dask.array
import numpy

from ..media_type_registration import deserialization_registry
from .base import BaseArrayClient
from .utils import export_util


class DaskArrayClient(BaseArrayClient):
    "Client-side wrapper around an array-like that returns dask arrays"

    def __init__(self, *args, item, route=None, **kwargs):
        if route is None:
            route = f"/{item['attributes']['structure_family']}/block"
        super().__init__(*args, route=route, item=item, **kwargs)

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
        if shape:
            # Check for special case of shape with 0 in it.
            if 0 in shape:
                # This is valid, and it has come up in the wild.  An array with
                # 0 as one of the dimensions never contains data, so we can
                # short-circuit here without any further information from the
                # service.
                return numpy.array([], dtype=dtype).reshape(shape)
            expected_shape = ",".join(map(str, shape))
        else:
            expected_shape = "scalar"
        content = self.context.get_content(
            self._route
            + "/"
            + "/".join(self.context.path_parts)
            + "/"
            + "/".join(self._path),
            headers={"Accept": media_type},
            params={
                "block": ",".join(map(str, block)),
                "expected_shape": expected_shape,
                **self._params,
            },
        )
        return deserialization_registry("array", media_type, content, dtype, shape)

    def read_block(self, block, slice=None):
        """
        Access the data for one block of this chunked (dask) array.

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
            f"{self.uri}"
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

    def export(self, filepath, format=None, slice=None, link=None, template_vars=None):
        """
        Download data in some format and write to a file.

        Parameters
        ----------
        file: str or buffer
            Filepath or writeable buffer.
        format : str, optional
            If format is None and `file` is a filepath, the format is inferred
            from the name, like 'table.csv' implies format="text/csv". The format
            may be given as a file extension ("csv") or a media type ("text/csv").
        slice : List[slice], optional
            List of slice objects. A convenient way to generate these is shown
            in the examples.
        link: str, optional
            Used internally. Refers to a key in the dictionary of links sent
            from the server.
        template_vars: dict, optional
            Used internally.

        Examples
        --------

        Export all.

        >>> a.export("numbers.csv")

        Export an N-dimensional slice.

        >>> import numpy
        >>> a.export("numbers.csv", slice=numpy.s_[:10, 50:100])
        """
        # For array, this is always 'full', but xarray clients set a custom link.
        if link is None:
            link = "full"
        template_vars = template_vars or {}
        params = {}
        if slice is not None:
            slices = []
            for dim in slice:
                if isinstance(dim, builtins.slice):
                    # slice(10, 50) -> "10:50"
                    # slice(None, 50) -> ":50"
                    # slice(10, None) -> "10:"
                    # slice(None, None) -> ":"
                    if (dim.step is not None) and dim.step != 1:
                        raise ValueError(
                            "Slices with a 'step' other than 1 are not supported."
                        )
                    slices.append(
                        (
                            (str(dim.start) if dim.start else "")
                            + ":"
                            + (str(dim.stop) if dim.stop else "")
                        )
                    )
                else:
                    slices.append(str(int(dim)))
            params["slice"] = ",".join(slices)
        return export_util(
            filepath,
            format,
            self.context.get_content,
            self.item["links"][link].format(**template_vars),
            params=params,
        )

    @property
    def formats(self):
        "List formats that the server can export this data as."
        return self.context.get_json("")["formats"]["array"]


class ArrayClient(DaskArrayClient):
    "Client-side wrapper around an array-like that returns in-memory arrays"

    def read(self, slice=None):
        """
        Acess the entire array or a slice.
        """
        return super().read(slice).compute()

    def read_block(self, block, slice=None):
        """
        Access the data for one block of this chunked array.

        Optionally, access only a slice *within* this block.
        """
        return super().read_block(block, slice).compute()

    def touch(self):
        # Do not run super().touch() because DaskArrayClient calls compute()
        # which does not apply here.
        BaseArrayClient.touch(self)
        self.read()
