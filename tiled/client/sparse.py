from ._protocol.sparse import (
    SparseReadRecipe,
    draft_write_block_request,
    draft_write_request,
)
from .base import BaseClient
from .utils import export_util, handle_error, params_from_slice, retry_context


class SparseClient(BaseClient):
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

    def __array__(self, *args, **kwargs):
        return self.read().__array__(*args, **kwargs)

    def todense(self, *args, **kwargs):
        "Return a dense numpy array. May be large."
        return self.read().todense(*args, **kwargs)

    def read_block(self, block, slice=None):
        recipe = SparseReadRecipe.for_block(
            self.item["links"]["block"], self.structure(), block, slice
        )
        for attempt in retry_context():
            with attempt:
                content = handle_error(self._send(recipe.draft)).read()
        return recipe.parse(content)

    def read(self, slice=None):
        recipe = SparseReadRecipe.for_full(
            self.item["links"]["full"], self.structure(), slice
        )
        for attempt in retry_context():
            with attempt:
                content = handle_error(self._send(recipe.draft)).read()
        return recipe.parse(content)

    def write(self, coords, data):
        draft = draft_write_request(self.item["links"]["full"], coords, data)
        for attempt in retry_context():
            with attempt:
                handle_error(self._send(draft))

    def write_block(self, coords, data, block):
        draft = draft_write_block_request(
            self.item["links"]["block"], block, coords, data
        )
        for attempt in retry_context():
            with attempt:
                handle_error(self._send(draft))

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
            self.context.http_client.get,
            self.item["links"]["full"],
            params=params,
        )
