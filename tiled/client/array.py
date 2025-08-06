import itertools
from typing import Union
from urllib.parse import parse_qs, urlparse

import dask
import dask.array
import httpx
import numpy
from numpy.typing import NDArray

from ..structures.core import STRUCTURE_TYPES
from .base import BaseClient
from .utils import (
    chunks_repr,
    export_util,
    handle_error,
    params_from_slice,
    retry_context,
)


class _DaskArrayClient(BaseClient):
    "Client-side wrapper around an array-like that returns dask arrays"

    def __init__(self, *args, item, **kwargs):
        super().__init__(*args, item=item, **kwargs)

    @property
    def dims(self):
        return self.structure().dims

    @property
    def shape(self):
        return self.structure().shape

    @property
    def size(self):
        return numpy.prod(self.structure().shape)

    @property
    def dtype(self):
        return self.structure().data_type.to_numpy_dtype()

    @property
    def nbytes(self):
        structure = self.structure()
        itemsize = structure.data_type.to_numpy_dtype().itemsize
        return numpy.prod(structure.shape) * itemsize

    @property
    def chunks(self):
        return self.structure().chunks

    @property
    def ndim(self):
        return len(self.structure().shape)

    def __repr__(self):
        structure = self.structure()
        attrs = {
            "shape": structure.shape,
            "chunks": chunks_repr(structure.chunks),
            "dtype": structure.data_type.to_numpy_dtype(),
        }
        if structure.dims:
            attrs["dims"] = structure.dims
        return (
            f"<{type(self).__name__}"
            + "".join(f" {k}={v}" for k, v in attrs.items())
            + ">"
        )

    def __array__(self, *args, **kwargs):
        return self.read().__array__(*args, **kwargs)

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
        url_path = self.item["links"]["block"]
        for attempt in retry_context():
            with attempt:
                content = handle_error(
                    self.context.http_client.get(
                        url_path,
                        headers={"Accept": media_type},
                        params={
                            **parse_qs(urlparse(url_path).query),
                            "block": ",".join(map(str, block)),
                            "expected_shape": expected_shape,
                        },
                    )
                ).read()
        return numpy.frombuffer(content, dtype=dtype).reshape(shape)

    def read_block(self, block, slice=None):
        """
        Access the data for one block of this chunked (dask) array.

        Optionally, access only a slice *within* this block.
        """
        structure = self.structure()
        chunks = structure.chunks
        dtype = structure.data_type.to_numpy_dtype()
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
        Access the entire array or a slice.

        The array will be internally chunked with dask.
        """
        structure = self.structure()
        shape = structure.shape
        dtype = structure.data_type.to_numpy_dtype()
        # Build a client-side dask array whose chunks pull from a server-side
        # dask array.
        name = "remote-dask-array-" f"{self.uri}"
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
        dask_array = dask.array.Array(
            dask=dask_tasks, name=name, chunks=chunks, dtype=dtype, shape=shape
        )
        if slice is not None:
            dask_array = dask_array[slice]
        return dask_array

    def write(self, array):
        for attempt in retry_context():
            with attempt:
                handle_error(
                    self.context.http_client.put(
                        self.item["links"]["full"],
                        content=array.tobytes(),
                        headers={"Content-Type": "application/octet-stream"},
                    )
                )

    def write_block(self, array, block, slice=...):
        url_path = self.item["links"]["block"].format(*block)
        params = {
            **parse_qs(urlparse(url_path).query),
            **params_from_slice(slice),
        }
        for attempt in retry_context():
            with attempt:
                handle_error(
                    self.context.http_client.put(
                        url_path,
                        content=array.tobytes(),
                        headers={"Content-Type": "application/octet-stream"},
                        params=params,
                    )
                )

    def patch(self, array: NDArray, offset: Union[int, tuple[int, ...]], extend=False):
        """
        Write data into a slice of an array, maybe extending the shape.

        Parameters
        ----------
        array : array-like
            The data to write
        offset : tuple[int, ...]
            Where to place this data in the array
        extend : bool
            Extend the array shape to fit the new slice, if necessary

        Examples
        --------

        Create a (3, 2, 2) array of ones.

        >>> ac = c.write_array(numpy.ones((3, 2, 2)), key='y')
        >>> ac
        <ArrayClient shape=(3, 2, 2) chunks=((3,), (2,), (2,)) dtype=float64>

        Read it.

        >>> ac.read()
        array([[[1., 1.],
                [1., 1.]],

               [[1., 1.],
                [1., 1.]],

               [[1., 1.],
                [1., 1.]]])

        Extend the array by concatenating a (1, 2, 2) array of zeros.

        >>> ac.patch(numpy.zeros((1, 2, 2)), offset=(3,), extend=True)

        Read it.

        >>> array([[[1., 1.],
                    [1., 1.]],

                   [[1., 1.],
                    [1., 1.]],

                   [[1., 1.],
                    [1., 1.]],

                   [[0., 0.],
                    [0., 0.]]])
        """
        if array.dtype != self.dtype:
            raise ValueError(
                f"Data given to patch has dtype {array.dtype} which does not "
                f"match the dtype of this array {self.dtype}."
            )
        array_ = numpy.ascontiguousarray(array)
        if isinstance(offset, int):
            offset = (offset,)
        url_path = self.item["links"]["full"]
        params = {
            **parse_qs(urlparse(url_path).query),
            "offset": ",".join(map(str, offset)),
            "shape": ",".join(map(str, array_.shape)),
            "extend": bool(extend),
        }
        for attempt in retry_context():
            with attempt:
                response = self.context.http_client.patch(
                    url_path,
                    content=array_.tobytes(),
                    headers={"Content-Type": "application/octet-stream"},
                    params=params,
                )
                if response.status_code == httpx.codes.CONFLICT:
                    raise ValueError(
                        f"Slice {slice} does not fit within current array shape. "
                        "Pass keyword argument extend=True to extend the array "
                        "dimensions to fit."
                    )
                handle_error(response)
        # Update cached structure.
        new_structure = response.json()
        structure_type = STRUCTURE_TYPES[self.structure_family]
        self._structure = structure_type.from_json(new_structure)

    def __getitem__(self, slice):
        return self.read(slice)

    # The default object.__iter__ works as expected here, no need to
    # implemented it specifically.

    def __len__(self):
        # As with numpy, len(arr) is the size of the zeroth axis.
        return self.structure().shape[0]

    def export(
        self, filepath, *, format=None, slice=None, link=None, template_vars=None
    ):
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
        params = params_from_slice(slice)
        return export_util(
            filepath,
            format,
            self.context.http_client.get,
            self.item["links"][link].format(**template_vars),
            params=params,
        )


# Subclass with a public class that adds the dask-specific methods.


class DaskArrayClient(_DaskArrayClient):
    "Client-side wrapper around an array-like that returns dask arrays"

    def compute(self):
        "Alias to client.read().compute()"
        return self.read().compute()


class ArrayClient(DaskArrayClient):
    "Client-side wrapper around an array-like that returns in-memory arrays"

    def read(self, slice=None):
        """
        Access the entire array or a slice.
        """
        return super().read(slice).compute()

    def read_block(self, block, slice=None):
        """
        Access the data for one block of this chunked array.

        Optionally, access only a slice *within* this block.
        """
        return super().read_block(block, slice).compute()
