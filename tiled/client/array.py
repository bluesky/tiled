import concurrent.futures
import math
from typing import TYPE_CHECKING, Optional, Union

import dask
import dask.array
import numpy
from numpy.typing import NDArray

from ..ndslice import NDBlock, NDSlice
from ..structures.core import structure_from_json
from ._protocol.array import (
    ArrayReadRecipe,
    draft_request_patch_array_full,
    draft_request_write_array_block,
    draft_request_write_array_full,
)
from ._protocol.request import RequestDraft
from .base import BaseClient
from .utils import (
    PATCH_CONFLICT_STATUS_CODES,
    ClientError,
    chunks_repr,
    export_util,
    params_from_slice,
    patch_conflict_message,
)

if TYPE_CHECKING:
    from .stream import ArraySubscription


class CommonArrayClient:
    """Pure array accessors shared by the synchronous and asynchronous clients.

    A mixin over `CommonBaseClient`: everything here reads the cached
    structure; nothing performs I/O.
    """

    # The limit on the expected size of the response body (before compression).
    # This will be used to determine how to combine multiple requests when fetching
    # data in blocks. If set to None, the client will not attempt to combine
    # requests and will fetch each chunk separately as determiied by the structure.
    RESPONSE_BYTESIZE_LIMIT = 100 * 1024 * 1024  # 100 MiB

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

    def __len__(self):
        # As with numpy, len(arr) is the size of the zeroth axis.
        return self.structure().shape[0]

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


class _DaskArrayClient(CommonArrayClient, BaseClient):
    "Client-side wrapper around an array-like that returns dask arrays"

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
        recipe = ArrayReadRecipe.for_block(
            self.item["links"]["block"], self.structure(), block, block_slice
        )
        if not recipe.drafts:
            # An array with 0 as one of its dimensions never contains data,
            # so we can short-circuit here without contacting the service.
            return recipe.empty()
        ((index, draft),) = recipe.drafts.items()
        return self._fetch_one(draft, recipe.dtype, recipe.shape_of_draft(index))

    def _fetch_one(self, draft: RequestDraft, dtype, expected_shape: tuple[int, ...]):
        """Send one read request and decode the response.

        The single executor for every array fetch, standalone (`read_block`)
        or as one task of a split read.
        """
        with self.context.throttle():
            content = self._send(draft, indicator_context=self.context).read()
        if (ps := self.context.progress_state) is not None:
            ps.advance()
        return ArrayReadRecipe.parse(content, dtype, expected_shape)

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
        recipe = ArrayReadRecipe.for_full(
            self.item["links"]["full"],
            self.structure(),
            NDSlice(slice),
            self.RESPONSE_BYTESIZE_LIMIT,
        )

        # An array with 0 as one of its dimensions never contains data, so
        # it is constructed locally without contacting the service.
        if not recipe.drafts:
            return dask.array.array([], dtype=recipe.dtype).reshape(recipe.shape)

        # A read expected to be small is fetched in one request.
        if len(recipe.drafts) == 1:
            (draft,) = recipe.drafts.values()
            return dask.array.from_delayed(
                dask.delayed(self._fetch_one)(draft, recipe.dtype, recipe.shape),
                dtype=recipe.dtype,
                shape=recipe.shape,
            )

        # The response is expected to be large; build a client-side dask
        # array whose chunks correspond to the recipe's sub-fetches.
        name = "remote-dask-array-" f"{self.uri}"
        dask_tasks = {
            (name,)
            + index: (
                self._fetch_one,
                draft,
                recipe.dtype,
                recipe.shape_of_draft(index),
            )
            for index, draft in recipe.drafts.items()
        }
        dask_array = dask.array.Array(
            name=name,
            dask=dask_tasks,
            dtype=recipe.dtype,
            chunks=recipe.tiling,
            shape=recipe.shape,
        )
        return dask_array

    def write(self, array, persist=True):
        draft = draft_request_write_array_full(
            self.item["links"]["full"], array, persist
        )
        self._send(draft)

    def write_block(self, array, block, slice=..., persist=True):
        draft = draft_request_write_array_block(
            self.item["links"]["block"], block, array, slice, persist
        )
        self._send(draft)

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
        draft = draft_request_patch_array_full(
            self.item["links"]["full"], array, offset, extend, persist
        )
        try:
            response = self._send(draft)
        except ClientError as err:
            if err.response.status_code in PATCH_CONFLICT_STATUS_CODES:
                raise ValueError(patch_conflict_message(err.response.json())) from err
            raise
        # Update cached structure.
        self._structure = structure_from_json(self.structure_family, response.json())

    def __getitem__(self, slice):
        return self.read(slice)

    # The default object.__iter__ works as expected here, no need to
    # implemented it specifically.

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
            self.context,
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
        dask_arr = self.read()
        # One fetch per chunk: the lazy array is chunked by sub-fetch.
        # A zero-size array is constructed locally, with no fetches at all.
        n = 0 if dask_arr.size == 0 else math.prod(len(c) for c in dask_arr.chunks)
        with self.context.tracking_progress(total=n):
            return dask_arr.compute()


class ArrayClient(DaskArrayClient):
    "Client-side wrapper around an array-like that returns in-memory arrays"

    def read(self, slice=None):
        """
        Access the entire array or a slice.
        """
        dask_arr = super().read(slice)
        # One fetch per chunk: the lazy array is chunked by sub-fetch.
        # A zero-size array is constructed locally, with no fetches at all.
        n = 0 if dask_arr.size == 0 else math.prod(len(c) for c in dask_arr.chunks)
        with self.context.tracking_progress(total=n):
            return dask_arr.compute()

    def read_block(self, block, slice=None):
        """
        Access the data for one block of this chunked array.

        Optionally, access only a slice *within* this block.
        """
        return super().read_block(block, slice).compute()
