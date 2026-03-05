import builtins
import concurrent.futures
import itertools
from typing import TYPE_CHECKING, Optional, Union
from urllib.parse import parse_qs, urlparse
from collections import defaultdict
from bisect import bisect_right, bisect_left

import dask
import dask.array
import httpx
import numpy
import math
from numpy.typing import NDArray

from ..structures.core import STRUCTURE_TYPES
from .base import BaseClient
from .utils import (
    chunks_repr,
    export_util,
    handle_error,
    params_from_slice,
    retry_context,
    balanced_merge
)
from ..ndslice import NDSlice, NDBlock

if TYPE_CHECKING:
    from .stream import ArraySubscription


class _DaskArrayClient(BaseClient):
    "Client-side wrapper around an array-like that returns dask arrays"

    # The limit on the expected size of the response body (before compression).
    # This will be used to determine how to combine multiple requests when fetching
    # data in blocks. If set to None, the client will not attempt to combine
    # requests and will fetch each chunk separately as determiied by the structure.
    RESPONSE_BYTESIZE_LIMIT = 250 * 1024 * 1024 +1 # 250 MiB

    def __init__(self, *args, item, **kwargs):
        super().__init__(*args, item=item, **kwargs)

    @staticmethod
    def dict_to_dask_chunks(chunk_dict):
        """
        Convert a dictionary mapping:
            {index_tuple: chunk_size_tuple}
        into Dask-style chunk representation:
            tuple of tuples, one per axis.
        """

        if not chunk_dict:
            return ()

        # Get dimensionality from the first key
        ndim = len(next(iter(chunk_dict)))

        # Validate dimensions
        for idx, chunk in chunk_dict.items():
            if len(idx) != ndim or len(chunk) != ndim:
                raise ValueError("Index and chunk dimensions must match")

        # Collect chunk sizes per axis, keyed by axis index
        axis_chunks = [defaultdict(int) for _ in range(ndim)]

        for idx, chunk in chunk_dict.items():
            for axis in range(ndim):
                axis_chunks[axis][idx[axis]] = chunk[axis]

        # Convert to ordered tuples (sorted by chunk index)
        dask_chunks = tuple(
            tuple(size for _, size in sorted(axis_dict.items()))
            for axis_dict in axis_chunks
        )

        return dask_chunks

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

    def _form_blocks(self, slice: NDSlice = NDSlice(...)) -> dict[tuple[int, ...], tuple[NDBlock, NDSlice]]:
        """Determine the most efficient way to fetch chunked data for a given slice
        
        Given a desired slice of the array, find which blocks (contiguous chunks) are needed
        to construct the desired output and what slices within those blocks are needed.
        """

        array_slice = slice.expand_for_shape(self.shape)
        expected_shape = array_slice.shape_after_slice(self.shape)

        # If possible, fetch the whole slice in one request
        total_bytes = math.prod(expected_shape) * self.dtype.itemsize
        if total_bytes < self.RESPONSE_BYTESIZE_LIMIT:
            return {(0,) * len(expected_shape): (NDBlock(...), slice)}

        # Find which chunks cover the requested slice and combine them into blocks
        # 1. Compute the cumulative boundaries of the chunks along each axis
        # 2. For each axis, use binary search to find the range of chunk indices
        #    that overlap with the requested "global" slice
        # 3. Find "local" slices within each chunk that overlap with the requested slice
        chunk_bounds = tuple(tuple(itertools.accumulate(axis_chunks, initial=0)) for axis_chunks in self.chunks)
        chunk_ranges = []  # Ranges-per-axis of indices of touched chunk
        for axis_bounds, axis_slice in zip(chunk_bounds, array_slice):
            start = bisect_left(axis_bounds, axis_slice.start + 1) - 1
            stop = bisect_right(axis_bounds, axis_slice.stop - 1)
            chunk_ranges.append(range(start, stop))

        # Iterate only over touched chunks and find the local slices
        sel_chunks: dict[NDBlock, NDSlice] = {}
        for chunk_indx in itertools.product(*chunk_ranges):
            local_slice = []
            for axis, cidx in enumerate(chunk_indx):
                chunk_start = chunk_bounds[axis][cidx]
                chunk_end = chunk_bounds[axis][cidx + 1]
                slc = array_slice[axis]

                # Compute overlap and convert to chunk-local coordinates
                overlap_start = max(chunk_start, slc.start)
                slc_last = slc.start + slc.step * ((slc.stop - slc.start) // slc.step)
                overlap_stop = min(chunk_end, slc_last+1, slc.stop)
                if overlap_start >= overlap_stop:
                    break  # no overlap
                start = overlap_start - chunk_start
                stop = overlap_stop - chunk_start
                local_slice.append(builtins.slice(start, stop, slc.step) if start != stop else start)
            else:
                sel_chunks[NDBlock(chunk_indx)] = NDSlice(local_slice)

        # Find indices of chunks at the corners, and compute the lengths
        # of chunks along each dimension, restricted to only selected chunks
        tl_indx = numpy.array(list(sel_chunks.keys())).min(axis=0)
        br_indx = numpy.array(list(sel_chunks.keys())).max(axis=0)
        lengths = [numpy.diff(bnd[tli:bri+2]) for bnd, tli, bri in zip(chunk_bounds, tl_indx, br_indx)]

        # 
        n_entries_max = self.RESPONSE_BYTESIZE_LIMIT / self.dtype.itemsize
        slice_to_block_indx = balanced_merge(lengths, vmax = 150000) #n_entries_max)
        for slice_ranges_per_block in itertools.product(*slice_to_block_indx):
            for slice_indx_range, tli in zip(slice_ranges_per_block, tl_indx):
                print(slice_indx_range, tli, slice_indx_range+tli)
        
        # TODO: Merge each dimension separately

        result = {tuple(((numpy.array(key) - tl_indx).tolist())): (key, slc) for key, slc in sel_chunks.items()}

        return result

    def _get_block(self, block: NDBlock, block_slice: Optional[NDSlice]=None):
        """
        Fetch the actual data for one chunk (or a block of chunks) in a chunked (dask) array.

        See read_block() for a public version of this. This private version
        enables more efficient multi-block access by requiring the caller to
        pass in the structure (dtype, shape).

        Parameters
        ----------
        block : NDBlock
            The chunk index, e.g. (0, 0), (0, 1), (0, 2) .... for a 2D array chunked into 3 blocks,
            or a slice object specifying a block of chunks, e.g. 0:2, 0:3 to get a block of the first 2 chunks
            along the first axis and all 3 chunks along the second axis.
        dtype : numpy.dtype
            The dtype of the array, needed to interpret the bytes returned from the server.
        shape : tuple[int, ...]
            The shape of this block, needed to reshape the 1D array returned from the server into the correct shape.
        block_slice : slice or tuple of slices, optional
            A slice within this block to return.
        """
        media_type = "application/octet-stream"
        structure = self.structure()
        dtype = structure.data_type.to_numpy_dtype()
        
        # Determine the expected shape of the resulting array after slicing
        expected_shape = []
        block = block.expand_for_shape([len(dim) for dim in structure.chunks])  # to convert for URL
        if shape := block.shape_from_chunks(structure.chunks):
            expected_shape = block_slice.shape_after_slice(shape) if block_slice else shape
            # Check for special case of shape with 0 in it.
            if 0 in expected_shape:
                # This is valid, and it has come up in the wild.  An array with
                # 0 as one of the dimensions never contains data, so we can
                # short-circuit here without any further information from the
                # service.
                return numpy.array([], dtype=dtype).reshape(expected_shape)

        url_path = self.item["links"]["block"]
        params={**parse_qs(urlparse(url_path).query),
                "block": block.to_numpy_str(),
                "expected_shape": ",".join(map(str, expected_shape)) or "scalar"
            }
        params = params | ( {"slice": block_slice.to_numpy_str()} if block_slice else {})
        for attempt in retry_context():
            with attempt:
                content = handle_error(
                    self.context.http_client.get(
                        url_path,
                        headers={"Accept": media_type},
                        params = params,
                    )
                ).read()
        return numpy.frombuffer(content, dtype=dtype).reshape(expected_shape)

    def read_block(self, block, slice=None):
        """
        Access the data for one block of this chunked (dask) array.

        Optionally, access only a slice *within* this block.
        """
        # structure = self.structure()
        # chunks = structure.chunks
        # dtype = structure.data_type.to_numpy_dtype()
        # try:
        #     shape = tuple(chunks[dim][i] for dim, i in enumerate(block))
        # except IndexError:
        #     raise IndexError(f"Block index {block} out of range")
        # dask_array = dask.array.from_delayed(
        #     dask.delayed(self._get_block)(block, dtype, shape), dtype=dtype, shape=shape
        # )
        # # TODO Make the request in _get_block include the slice so that we only
        # # fetch exactly the data that we want. This will require careful code
        # # to determine what the shape will be.
        # if slice is not None:
        #     dask_array = dask_array[slice]
        # return dask_array
        raise NotImplementedError("Block-wise fetching for slices is not yet implemented.")

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

        # Form the dask array out of the fetched blocks, assuming each block becomes a chunk.
        indexed_blocks = self._form_blocks(NDSlice(slice))  # {indx: (NDBlock, NDSlice)}
        block_shapes = {indx: block_slice.shape_after_slice(block.shape_from_chunks(structure.chunks)) for indx, (block, block_slice) in indexed_blocks.items()}
        final_chunks = self.dict_to_dask_chunks(block_shapes)
        final_shape = tuple(sum(ch) for ch in final_chunks)
        dask_tasks = {
            (name,)+ indx: (self._get_block, *block_and_slice) for indx, block_and_slice in indexed_blocks.items()
        }
        dask_array = dask.array.Array(
            dask=dask_tasks, name=name, dtype=dtype, chunks=final_chunks, shape=final_shape
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
        return self.read().compute()


class ArrayClient(DaskArrayClient):
    "Client-side wrapper around an array-like that returns in-memory arrays"

    def read(self, slice=None):
        """
        Access the entire array or a slice.
        """
        return super().read(slice or NDSlice()).compute()

    def read_block(self, block, slice=None):
        """
        Access the data for one block of this chunked array.

        Optionally, access only a slice *within* this block.
        """
        return super().read_block(block, slice).compute()
