import concurrent.futures
import itertools
import math
from typing import TYPE_CHECKING, Optional, Union
from urllib.parse import parse_qs, urlparse

import dask
import dask.array
import httpx
import numpy
from numpy.typing import NDArray

from ..ndslice import NDBlock, NDSlice
from ..structures.core import STRUCTURE_TYPES
from .base import BaseClient
from .utils import (
    chunks_repr,
    export_util,
    handle_error,
    params_from_slice,
    retry_context,
    slices_to_dask_chunks,
    split_nd_slice,
)

if TYPE_CHECKING:
    from .stream import ArraySubscription


class _DaskArrayClient(BaseClient):
    "Client-side wrapper around an array-like that returns dask arrays"

    # The limit on the expected size of the response body (before compression).
    # This will be used to determine how to combine multiple requests when fetching
    # data in blocks. If set to None, the client will not attempt to combine
    # requests and will fetch each chunk separately as determiied by the structure.
    RESPONSE_BYTESIZE_LIMIT = 250 * 1024 * 1024  # 250 MiB

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
        attrs = {
            "shape": self.shape,
            "chunks": chunks_repr(self.chunks),
            "dtype": self.dtype,
        }
        if dims := self.structure().dims:
            attrs["dims"] = dims
        return (
            f"<{type(self).__name__}"
            + "".join(f" {k}={v}" for k, v in attrs.items())
            + ">"
        )

    def __array__(self, *args, **kwargs):
        return self.read().__array__(*args, **kwargs)

    def _get_block(self, block: NDBlock, block_slice: Optional[NDSlice] = None):
        """Fetch the data for one chunk (block) in a chunked array.

        This private method is used internally by the client and requires the
        `block` and `block_slice` arguments to be pre-cast as NDBlock and NDSlice
        types, respectively.

        This method uses the `/array/block` endpoint to fetch one block of the array,
        at a time. The block boundaries are determined by the structure of the array,
        and usually correspond to the chunking of the array on the server side.

        See read_block() for a public version of this.

        Parameters
        ----------
        block : NDBlock
            The chunk index, e.g. (0, 0), (0, 1), (0, 2) .... for a 2D array
            chunked into 3 blocks.
        block_slice : NDSlice, optional
            A slice within this block to return.
        """

        media_type = "application/octet-stream"

        # Determine the expected shape of the resulting array after slicing
        exp_shape = []
        # Expand the block to convert for URL
        block = block.expand_for_shape([len(dim) for dim in self.chunks])
        if shape := block.shape_from_chunks(self.chunks):
            exp_shape = block_slice.shape_after_slice(shape) if block_slice else shape
            # Check for special case of shape with 0 in it.
            if 0 in exp_shape:
                # This is valid, and it has come up in the wild.  An array with
                # 0 as one of the dimensions never contains data, so we can
                # short-circuit here without any further information from the
                # service.
                return numpy.array([], dtype=self.dtype).reshape(exp_shape)

        url_path = self.item["links"]["block"]
        params = {
            **parse_qs(urlparse(url_path).query),
            "block": block.to_numpy_str(),
            "expected_shape": ",".join(map(str, exp_shape)) or "scalar",
        }
        params = params | ({"slice": block_slice.to_numpy_str()} if block_slice else {})
        for attempt in retry_context():
            with attempt:
                content = handle_error(
                    self.context.http_client.get(
                        url_path,
                        headers={"Accept": media_type},
                        params=params,
                    )
                ).read()

        return numpy.frombuffer(content, dtype=self.dtype).reshape(exp_shape)

    def _get_slice(self, slice: Optional[NDSlice] = None):
        """Fetch the data for a slice of the full array

        This private method is used internally by the client and requires the
        `slice` argument to be pre-cast as an NDSlice type.

        The request is made to the `/array/full` endpoint.

        See read() for a public version of this.

        Parameters
        ----------
        slice : NDSlice, optional
            A slice of the full array to return.
        """

        media_type = "application/octet-stream"

        # Determine the expected shape of the resulting array after slicing
        exp_shape = slice.shape_after_slice(self.shape) if slice else self.shape

        # Check for special case of shape with 0 in it.
        if 0 in exp_shape:
            # This is valid, and it has come up in the wild.  An array with
            # 0 as one of the dimensions never contains data, so we can
            # short-circuit here without any further information from the service.
            return numpy.array([], dtype=self.dtype).reshape(exp_shape)

        url_path = self.item["links"]["full"]
        params = {
            **parse_qs(urlparse(url_path).query),
            "expected_shape": ",".join(map(str, exp_shape)) or "scalar",
        }
        params = params | ({"slice": slice.to_numpy_str()} if slice else {})
        for attempt in retry_context():
            with attempt:
                content = handle_error(
                    self.context.http_client.get(
                        url_path,
                        headers={"Accept": media_type},
                        params=params,
                    )
                ).read()

        return numpy.frombuffer(content, dtype=self.dtype).reshape(exp_shape)

    def read_block(self, block, slice=None):
        """Access the data for one block of this chunked (dask) array.

        This method uses the `/array/block` endpoint to fetch one block of the array,
        at a time. The block boundaries are determined by the structure of the array,
        and usually correspond to the chunking of the array on the server side.

        Optionally, access only a slice *within* this block.
        """

        block, block_slice = NDBlock(block), NDSlice(slice)

        try:
            shape = block.shape_from_chunks(self.chunks)
        except IndexError:
            raise IndexError(f"Block index {block} out of range")

        exp_shape = block_slice.shape_after_slice(shape) if block_slice else shape
        dask_array = dask.array.from_delayed(
            dask.delayed(self._get_block)(block, block_slice),
            dtype=self.dtype,
            shape=exp_shape,
        )
        return dask_array

    def read(self, slice=None):
        """Access the entire array or its slice

        The array will be internally chunked with dask.
        """

        # Determine the expected shape of the resulting array after slicing
        if arr_slice := NDSlice(slice):
            arr_slice = arr_slice.expand_for_shape(self.shape)  # Remove "..."
        exp_shape = arr_slice.shape_after_slice(self.shape)
        total_bytes = math.prod(exp_shape) * self.dtype.itemsize

        # Check for special case of shape with 0 in it.
        if 0 in exp_shape:
            # This is valid, and it has come up in the wild.  An array with
            # 0 as one of the dimensions never contains data, so we can
            # short-circuit here without any further information from the service.
            return dask.array.array([], dtype=self.dtype).reshape(exp_shape)

        # If the expected response is small, fetch it in one go.
        if total_bytes < self.RESPONSE_BYTESIZE_LIMIT:
            dask_array = dask.array.from_delayed(
                dask.delayed(self._get_slice)(arr_slice),
                dtype=self.dtype,
                shape=exp_shape,
            )
            return dask_array

        # The response is expected to be large, subslice it and recombine with dask
        # Build chunk boundaries along each axis to find best candidate split points
        chunk_bounds = tuple(
            tuple(itertools.accumulate(axis_chunks, initial=0))
            for axis_chunks in self.chunks
        )
        indexed_slices = split_nd_slice(
            arr_slice.expand_for_shape(self.shape),
            max_size=self.RESPONSE_BYTESIZE_LIMIT // self.dtype.itemsize,
            pref_splits=chunk_bounds,
        )

        # Build a client-side dask array whose chunks correspond to subsplits of the slice
        name = "remote-dask-array-" f"{self.uri}"
        dask_tasks = {
            (name,) + indx: (self._get_slice, slc)
            for indx, (slc) in indexed_slices.items()
        }
        dask_array = dask.array.Array(
            name=name,
            dask=dask_tasks,
            dtype=self.dtype,
            chunks=slices_to_dask_chunks(indexed_slices, self.shape),
            shape=exp_shape,
        )
        return dask_array

    def write(self, array, persist=True):
        params = {}
        if persist is False:
            # Extend the query only for non-default behavior.
            params["persist"] = persist
        for attempt in retry_context():
            with attempt:
                handle_error(
                    self.context.http_client.put(
                        self.item["links"]["full"],
                        content=array.tobytes(),
                        headers={"Content-Type": "application/octet-stream"},
                        params=params,
                    )
                )

    def write_block(self, array, block, slice=..., persist=True):
        url_path = self.item["links"]["block"].format(*block)
        params = {
            **parse_qs(urlparse(url_path).query),
            **params_from_slice(slice),
        }
        if persist is False:
            # Extend the query only for non-default behavior.
            params["persist"] = persist
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

    def patch(
        self,
        array: NDArray,
        offset: Union[int, tuple[int, ...]],
        extend=False,
        persist=True,
    ):
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
        persist : bool | None
            Persist the changes on server storage if True. [default behavior]
            If False, the update is still streamed to subscribed listeners.

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
        if persist is False:
            # Extend the query only for non-default behavior.
            params["persist"] = persist
        for attempt in retry_context():
            with attempt:
                response = self.context.http_client.patch(
                    url_path,
                    content=array_.tobytes(),
                    headers={"Content-Type": "application/octet-stream"},
                    params=params,
                )
                if response.status_code in [
                    httpx.codes.BAD_REQUEST,
                    httpx.codes.CONFLICT,
                ]:
                    raise ValueError(
                        response.json()
                        .get("detail", "Array parameters conflict.")
                        .replace(
                            "Use ?",  # URL query param
                            "Pass keyword argument ",  # Python function argument
                        )
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

    def subscribe(
        self,
        executor: Optional[concurrent.futures.Executor] = None,
    ) -> "ArraySubscription":
        """
        Subscribe to streaming updates about this array.

        Parameters
        ----------
        executor : concurrent.futures.Executor, optional
            Launches tasks asynchronously, in response to updates. By default,
            a concurrent.futures.ThreadPoolExecutor is used.

        Returns
        -------
        subscription : ArraySubscription
        """
        # Keep this import here to defer the websockets import until/unless needed.
        from .stream import ArraySubscription

        return ArraySubscription(self.context, self.path_parts, executor)


# Subclass with a public class that adds the dask-specific methods.


class DaskArrayClient(_DaskArrayClient):
    "Client-side wrapper around an array-like that returns dask arrays"

    def compute(self):
        "Alias to client.read().compute()"
        arr = self.read().compute()
        return arr.item() if arr.shape == () else arr


class ArrayClient(DaskArrayClient):
    "Client-side wrapper around an array-like that returns in-memory arrays"

    def read(self, slice=None):
        """
        Access the entire array or a slice.
        """
        arr = super().read(slice).compute()
        return arr.item() if arr.shape == () else arr

    def read_block(self, block, slice=None):
        """
        Access the data for one block of this chunked array.

        Optionally, access only a slice *within* this block.
        """
        arr = super().read_block(block, slice).compute()
        return arr.item() if arr.shape == () else arr
