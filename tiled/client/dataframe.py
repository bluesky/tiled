import dask.dataframe

from ..containers.dataframe import (
    APACHE_ARROW_FILE_MIME_TYPE,
    DataFrameMacroStructure,
    DataFrameMicroStructure,
    DataFrameStructure,
)
from ..media_type_registration import deserialization_registry
from .base import BaseClientReader
from .utils import handle_error


class ClientDaskDataFrameReader(BaseClientReader):
    "Client-side wrapper around an array-like that returns dask arrays"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def structure(self):
        meta_response = self._client.get(
            f"/dataframe/meta/{'/'.join(self._path)}",
            params=self._params,
        )
        handle_error(meta_response)
        meta = deserialization_registry(
            "dataframe", APACHE_ARROW_FILE_MIME_TYPE, meta_response.content
        )
        divisions_response = self._client.get(
            f"/dataframe/divisions/{'/'.join(self._path)}",
            params=self._params,
        )
        handle_error(divisions_response)
        divisions = deserialization_registry(
            "dataframe", APACHE_ARROW_FILE_MIME_TYPE, divisions_response.content
        )
        return DataFrameStructure(
            micro=DataFrameMicroStructure(meta=meta, divisions=divisions),
            # We could get the macrostructure by making another HTTP request
            # but it's knowable from the microstructure, so we'll just recreate it
            # that way.
            macro=DataFrameMacroStructure(
                npartitions=len(divisions) - 1, columns=meta.columns
            ),
        )

    def read_partition(self, partition, columns):
        """
        Fetch the data for one block in a chunked (dask) array.
        """
        params = {"partition": partition}
        if columns:
            # Note: The singular/plural inconsistency here is due to the fact that
            # ["A", "B"] will be encoded in the URL as column=A&column=B
            params["column"] = columns
        response = self._client.get(
            "/dataframe/partition/" + "/".join(self._path),
            headers={"Accept": APACHE_ARROW_FILE_MIME_TYPE},
            params={**params, **self._params},
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
            + (partition,): (
                self.read_partition,
                partition,
                columns,
            )
            for partition in range(structure.macro.npartitions)
        }
        ddf = dask.dataframe.DataFrame(
            dask_tasks,
            name=name,
            meta=structure.micro.meta,
            divisions=structure.micro.divisions,
        )
        if columns is not None:
            ddf = ddf[columns]
        return ddf

    # We implement *some* of the Mapping interface here but intentionally not
    # all of it. DataFrames are not quite Mapping-like. Their __len__ for
    # example returns the number of rows (which it would be costly for us to
    # compute) as opposed to holding to the usual invariant
    # `len(list(obj)) == # len(obj)` for Mappings. Additionally, their behavior
    # with `__getitem__` is a bit "extra", e.g. df[["A", "B"]].

    def __getitem__(self, columns):
        if isinstance(columns, str):
            # Return a single column (a pandas.Series)
            return self.read(columns=[columns])[columns]
        else:
            # Return a DataFrame, with possibly a subset of the available columns.
            return self.read(columns=columns)

    def __iter__(self):
        yield from self.structure().macro.columns

    # __len__ is intentionally not implemented. For DataFrames it means "number
    # of rows" which is expensive to compute.


class ClientDataFrameReader(ClientDaskDataFrameReader):
    "Client-side wrapper around a dataframe-like that returns in-memory dataframes"

    def read(self, columns=None):
        return super().read(columns).compute()
