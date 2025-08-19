from urllib.parse import parse_qs, urlparse

import numpy
import sparse
from ndindex import ndindex

from ..serialization.table import deserialize_arrow, serialize_arrow
from ..utils import APACHE_ARROW_FILE_MIME_TYPE
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
        # Fetch the data as an Apache Arrow table
        # with columns named dim0, dim1, ..., dimN, data.
        structure = self.structure()
        url_path = self.item["links"]["block"]
        params = {**parse_qs(urlparse(url_path).query), **params_from_slice(slice)}
        params["block"] = ",".join(map(str, block))
        for attempt in retry_context():
            with attempt:
                content = handle_error(
                    self.context.http_client.get(
                        url_path,
                        headers={"Accept": APACHE_ARROW_FILE_MIME_TYPE},
                        params=params,
                    )
                ).read()
        df = deserialize_arrow(content)
        original_shape = structure.shape
        if slice is not None:
            sliced_shape = ndindex(slice).newshape(original_shape)
        else:
            sliced_shape = original_shape
        ndim = len(sliced_shape)

        return sparse.COO(
            data=df["data"].values,
            coords=numpy.stack([df[f"dim{i}"].values for i in range(ndim)]),
            shape=sliced_shape,
        )

    def read(self, slice=None):
        # Fetch the data as an Apache Arrow table
        # with columns named dim0, dim1, ..., dimN, data.
        structure = self.structure()
        url_path = self.item["links"]["full"]
        params = {**parse_qs(urlparse(url_path).query), **params_from_slice(slice)}
        for attempt in retry_context():
            with attempt:
                content = handle_error(
                    self.context.http_client.get(
                        url_path,
                        headers={"Accept": APACHE_ARROW_FILE_MIME_TYPE},
                        params=params,
                    )
                ).read()
        df = deserialize_arrow(content)
        original_shape = structure.shape
        if slice is not None:
            sliced_shape = ndindex(slice).newshape(original_shape)
        else:
            sliced_shape = original_shape
        ndim = len(sliced_shape)

        return sparse.COO(
            data=df["data"].values,
            coords=numpy.stack([df[f"dim{i}"].values for i in range(ndim)]),
            shape=sliced_shape,
        )

    def write(self, coords, data):
        import pandas

        d = {f"dim{i}": coords for i, coords in enumerate(coords)}
        d["data"] = data
        df = pandas.DataFrame(d)
        for attempt in retry_context():
            with attempt:
                handle_error(
                    self.context.http_client.put(
                        self.item["links"]["full"],
                        content=bytes(
                            serialize_arrow(APACHE_ARROW_FILE_MIME_TYPE, df, {})
                        ),
                        headers={"Content-Type": APACHE_ARROW_FILE_MIME_TYPE},
                    )
                )

    def write_block(self, coords, data, block):
        import pandas

        d = {f"dim{i}": coords for i, coords in enumerate(coords)}
        d["data"] = data
        df = pandas.DataFrame(d)
        for attempt in retry_context():
            with attempt:
                handle_error(
                    self.context.http_client.put(
                        self.item["links"]["block"].format(*block),
                        content=bytes(
                            serialize_arrow(APACHE_ARROW_FILE_MIME_TYPE, df, {})
                        ),
                        headers={"Content-Type": APACHE_ARROW_FILE_MIME_TYPE},
                    )
                )

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
