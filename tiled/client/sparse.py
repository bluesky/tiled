import numpy
import sparse
from ndindex import ndindex

from ..serialization.dataframe import deserialize_arrow
from ..utils import APACHE_ARROW_FILE_MIME_TYPE
from .base import BaseStructureClient
from .utils import export_util, params_from_slice


class SparseClient(BaseStructureClient):
    def read_block(self, block, slice=None):
        # Fetch the data as an Apache Arrow table
        # with columns named dim0, dim1, ..., dimN, data.
        structure = self.structure()
        params = params_from_slice(slice)
        params["block"] = ",".join(map(str, block))
        content = self.context.get_content(
            self.item["links"]["block"],
            accept=APACHE_ARROW_FILE_MIME_TYPE,
            params=params,
        )
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
        params = params_from_slice(slice)
        content = self.context.get_content(
            self.item["links"]["full"],
            accept=APACHE_ARROW_FILE_MIME_TYPE,
            params=params,
        )
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
            List of slice objects. A convenient way to generate these is shown
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
            self.context.get_content,
            self.item["links"]["full"],
            params=params,
        )
