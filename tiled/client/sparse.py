import sparse

from .base import BaseStructureClient
from .utils import export_util


class SparseClient(BaseStructureClient):
    def read(self, slice=None):
        structure = self.structure()
        ArrayClient = self.structure_clients["array"]
        data_item = {
            "metadata": {},
            "path": self.path + ["/data"],
            "specs": [],
            "links": {"self": self.uri + "/data"},
        }
        coords_item = {
            "metadata": {},
            "path": self.path + ["/coords"],
            "specs": [],
            "links": {"self": self.uri + "/coords"},
        }
        data_client = ArrayClient(
            self.context,
            item=data_item,
            structure=structure.data,
            path=data_item["path"],
            structure_clients=self.structure_clients,
        )
        coords_client = ArrayClient(
            self.context,
            item=coords_item,
            structure=structure.coords,
            path=coords_item["path"],
            structure_clients=self.structure_clients,
        )
        arr = sparse.COO(
            data=data_client.read(),
            coords=coords_client.read(),
            shape=structure.shape,
        )
        # TODO Slice server-side as we do with dense arrays.
        return arr[slice]

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
            Not implemented yet for sparse arrays

        Examples
        --------

        Export all.

        >>> a.export("numbers.csv")
        """
        params = {}
        return export_util(
            filepath,
            format,
            self.context.get_content,
            self.item["links"]["full"],
            params=params,
        )
