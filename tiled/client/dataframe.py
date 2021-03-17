import dask.dataframe
import pyarrow

from ..containers.dataframe import APACHE_ARROW_FILE_MIME_TYPE, DataFrameStructure
from ..media_type_registration import deserialization_registry
from .base import BaseClientReader
from .utils import handle_error


class ClientDaskDataFrameReader(BaseClientReader):
    "Client-side wrapper around an array-like that returns dask arrays"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def structure(self):
        # Notice that we are NOT *caching* in self._structure here. We are
        # allowing that the creator of this instance might have already known
        # our structure (as part of the some larger structure) and passed it
        # in.
        if self._structure is None:
            response = self._client.get(
                f"/dataframe/schema/{'/'.join(self._path)}",
                params=self._params,
            )
            handle_error(response)
            meta = pyarrow.deserialize_pandas(response.content)
        else:
            structure = self._structure
        return structure

    def read_partition(self, partition, columns):
        """
        Fetch the data for one block in a chunked (dask) array.
        """
        response = self._client.get(
            "/dataframe/partition" + "/".join(self._path),
            headers={"Accept": APACHE_ARROW_FILE_MIME_TYPE},
            params={"partition": partition, "columns": columns, **self._params},
        )
        handle_error(response)
        return deserialization_registry(
            "dataframe", APACHE_ARROW_FILE_MIME_TYPE, response.content
        )

    def read(self, columns=None):
        structure = self.structure()
        # Build a client-side dask dataframe whose partitions pull from a
        # server-side dask array.
        name = (
            "remote-dask-dataframe-"
            f"{self._client.base_url!s}/{'/'.join(self._path)}"
            f"{'-'.join(map(repr, sorted(self._params.items())))}"
        )
        dask_tasks = {
            (name,)
            + partition: (
                self.read_partition,
                partition,
                columns,
            )
            for partition in range(len(structure.divisions))
        }
        return dask.array.Array(
            dask=dask_tasks,
            name=name,
            meta=structure.meta,
            divisions=structure.divisions,
        )


class ClientDataFrameReader(ClientDaskDataFrameReader):
    "Client-side wrapper around an array-like that returns in-memory arrays"

    def read(self):
        return super().read().compute()
