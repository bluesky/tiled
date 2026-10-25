from ._protocol.sparse import (
    SparseReadRecipe,
    draft_request_write_sparse_block,
    draft_request_write_sparse_full,
)
from .base import BaseClient
from .utils import export_util, params_from_slice


class CommonSparseClient:
    """Pure sparse-array accessors and glue shared by the sync and async clients.

    A mixin over `CommonBaseClient`: everything here reads the cached
    structure; nothing performs I/O.
    """

    @property
    def dims(self):
        return self.structure().dims

    @property
    def shape(self):
        return self.structure().shape

    @property
    def chunks(self):
        return self.structure().chunks

    @property
    def ndim(self):
        return len(self.structure().shape)

    def __repr__(self):
        structure = self.structure()
        attrs = {"shape": structure.shape, "chunks": structure.chunks}
        if structure.dims:
            attrs["dims"] = structure.dims
        return (
            f"<{type(self).__name__}"
            + "".join(f" {k}={v}" for k, v in attrs.items())
            + ">"
        )

    # -- Glue between instance state and the protocol layer ------------------
    # Pure methods that bind this client's state (item, structure) to the
    # builders and parsers in `tiled.client._protocol`. The sync and async
    # shells share them; only the sends in between differ.

    def _read_recipe(self, slice=None):
        "A recipe that reads the whole array or a slice of it."
        return SparseReadRecipe.for_full(
            self.item["links"]["full"], self.structure(), slice
        )

    def _draft_request_write(self, coords, data):
        "Draft the request that overwrites the array in full."
        return draft_request_write_sparse_full(self.item["links"]["full"], coords, data)


class SparseClient(CommonSparseClient, BaseClient):
    def _read_block_recipe(self, block, slice=None):
        "A recipe that reads one block (chunk) of the array."
        return SparseReadRecipe.for_block(
            self.item["links"]["block"], self.structure(), block, slice
        )

    def __array__(self, *args, **kwargs):
        return self.read().__array__(*args, **kwargs)

    def todense(self, *args, **kwargs):
        "Return a dense numpy array. May be large."
        return self.read().todense(*args, **kwargs)

    def read_block(self, block, slice=None):
        recipe = self._read_block_recipe(block, slice)
        content = self._send(recipe.draft).read()
        return recipe.parse_response(content)

    def read(self, slice=None):
        recipe = self._read_recipe(slice)
        content = self._send(recipe.draft).read()
        return recipe.parse_response(content)

    def write(self, coords, data):
        self._send(self._draft_request_write(coords, data))

    def write_block(self, coords, data, block):
        draft = draft_request_write_sparse_block(
            self.item["links"]["block"], block, coords, data
        )
        self._send(draft)

    def __getitem__(self, slice):
        return self.read(slice)

    def export(self, filepath, *, format=None, slice=None):
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
            List of slice objects. A convenient way to generate these are shown
            in the examples.

        Examples
        --------

        Export all.

        >>> a.export("numbers.csv")

        Export an N-dimensional slice.

        >>> import numpy
        >>> a.export("numbers.csv", slice=numpy.s_[:10, 50:100])
        """
        params = params_from_slice(slice)
        return export_util(
            filepath,
            format,
            self.context,
            self.item["links"]["full"],
            params=params,
        )
