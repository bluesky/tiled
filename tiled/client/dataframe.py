import dask
import dask.dataframe

from ..structures.dataframe import (
    APACHE_ARROW_FILE_MIME_TYPE,
    DataFrameMacroStructure,
    DataFrameMicroStructure,
    DataFrameStructure,
)
from ..media_type_registration import deserialization_registry
from .base import BaseStructureClient
from .utils import export_util


class DaskDataFrameClient(BaseStructureClient):
    "Client-side wrapper around an array-like that returns dask arrays"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _repr_pretty_(self, p, cycle):
        """
        Provide "pretty" display in IPython/Jupyter.

        See https://ipython.readthedocs.io/en/stable/config/integrating.html#rich-display
        """
        # Try to get the column names, but give up quickly to avoid blocking
        # for long.
        TIMEOUT = 0.2  # seconds
        try:
            content = self.context.get_json(
                self.uri,
                params={"fields": "structure.macro", **self._params},
                timeout=TIMEOUT,
            )
        except TimeoutError:
            p.text(
                f"<{type(self).__name__} Loading column names took too long; use list(...) >"
            )
        except Exception as err:
            p.text(f"<{type(self).__name__} Loading column names raised error {err!r}>")
        else:
            try:
                columns = content["data"]["attributes"]["structure"]["macro"]["columns"]
            except Exception as err:
                p.text(
                    f"<{type(self).__name__} Loading column names raised error {err!r}>"
                )
            else:
                p.text(f"<{type(self).__name__} {columns}>")

    def _ipython_key_completions_(self):
        """
        Provide method for the key-autocompletions in IPython.

        See http://ipython.readthedocs.io/en/stable/config/integrating.html#tab-completion
        """
        try:
            content = self.context.get_json(
                self.uri,
                params={"fields": "structure.macro", **self._params},
            )
            columns = content["data"]["attributes"]["structure"]["macro"]["columns"]
        except Exception:
            # Do not print messy traceback from thread. Just fail silently.
            return []
        return columns

    def touch(self):
        super().touch()
        self._ipython_key_completions_()
        self.read().compute()

    def structure(self):
        meta_content = self.context.get_content(
            f"/dataframe/meta/{'/'.join(self.context.path_parts)}/{'/'.join(self._path)}",
            params=self._params,
        )
        meta = deserialization_registry(
            "dataframe", APACHE_ARROW_FILE_MIME_TYPE, meta_content
        )
        divisions_content = self.context.get_content(
            f"/dataframe/divisions/{'/'.join(self.context.path_parts)}/{'/'.join(self._path)}",
            params=self._params,
        )
        divisions_wrapped_in_df = deserialization_registry(
            "dataframe", APACHE_ARROW_FILE_MIME_TYPE, divisions_content
        )
        divisions = tuple(divisions_wrapped_in_df["divisions"].values)
        return DataFrameStructure(
            micro=DataFrameMicroStructure(meta=meta, divisions=divisions),
            # We could get the macrostructure by making another HTTP request
            # but it's knowable from the microstructure, so we'll just recreate it
            # that way.
            macro=DataFrameMacroStructure(
                npartitions=len(divisions) - 1, columns=meta.columns
            ),
        )

    def _get_partition(self, partition, columns):
        """
        Fetch the actual data for one partition in a partitioned (dask) dataframe.

        See read_partition for a public version of this.
        """
        params = {"partition": partition}
        if columns:
            # Note: The singular/plural inconsistency here is due to the fact that
            # ["A", "B"] will be encoded in the URL as column=A&column=B
            params["column"] = columns
        content = self.context.get_content(
            f"/dataframe/partition/{'/'.join(self.context.path_parts)}/{'/'.join(self._path)}",
            headers={"Accept": APACHE_ARROW_FILE_MIME_TYPE},
            params={**params, **self._params},
        )
        return deserialization_registry(
            "dataframe", APACHE_ARROW_FILE_MIME_TYPE, content
        )

    def read_partition(self, partition, columns=None):
        """
        Access one partition in a partitioned (dask) dataframe.

        Optionally select a subset of the columns.
        """
        structure = self.structure()
        npartitions = structure.macro.npartitions
        if not (0 <= partition < npartitions):
            raise IndexError(f"partition {partition} out of range")
        meta = structure.micro.meta
        if columns is not None:
            meta = meta[columns]
        return dask.dataframe.from_delayed(
            [dask.delayed(self._get_partition)(partition, columns)],
            meta=meta,
            divisions=structure.micro.divisions,
        )

    def read(self, columns=None):
        """
        Access the entire DataFrame. Optionally select a subset of the columns.

        The result will be internally partitioned with dask.
        """
        structure = self.structure()
        # Build a client-side dask dataframe whose partitions pull from a
        # server-side dask array.
        name = (
            "remote-dask-dataframe-"
            f"{self.context.base_url!s}/{'/'.join(self._path)}"
            f"{'-'.join(map(repr, sorted(self._params.items())))}"
        )
        dask_tasks = {
            (name,)
            + (partition,): (
                self._get_partition,
                partition,
                columns,
            )
            for partition in range(structure.macro.npartitions)
        }
        meta = structure.micro.meta
        if columns is not None:
            meta = meta[columns]
        ddf = dask.dataframe.DataFrame(
            dask_tasks,
            name=name,
            meta=meta,
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
        # This is type unstable, matching pandas' behavior.
        if isinstance(columns, str):
            # Return a single column (a pandas.Series)
            return self.read(columns=[columns])[columns]
        else:
            # Return a DataFrame with a subset of the available columns.
            return self.read(columns=columns)

    def __iter__(self):
        yield from self.structure().macro.columns

    # __len__ is intentionally not implemented. For DataFrames it means "number
    # of rows" which is expensive to compute.

    def export(self, filepath, format=None, columns=None):
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
        columns: List[str], optional
            Select a subset of the columns.
        """
        params = {}
        if columns is not None:
            params["column"] = columns
        return export_util(
            filepath,
            format,
            self.context.get_content,
            self.item["links"]["full"],
            params=params,
        )

    @property
    def formats(self):
        "List formats that the server can export this data as."
        return self.context.get_json("")["formats"]["dataframe"]


class DataFrameClient(DaskDataFrameClient):
    "Client-side wrapper around a dataframe-like that returns in-memory dataframes"

    def read_partition(self, partition, columns=None):
        """
        Access one partition of the DataFrame. Optionally select a subset of the columns.
        """
        return super().read_partition(partition, columns).compute()

    def read(self, columns=None):
        """
        Access the entire DataFrame. Optionally select a subset of the columns.
        """
        if self.structure().macro.npartitions == 1:
            # Fast path: download the entire dataframe using /dataframe/full/
            content = self.context.get_content(
                f"/dataframe/full/{'/'.join(self.context.path_parts)}/{'/'.join(self._path)}",
                params=self._params,
                headers={"Accept": APACHE_ARROW_FILE_MIME_TYPE},
                )
            return deserialization_registry(
                "dataframe", APACHE_ARROW_FILE_MIME_TYPE, content
                )
        
        return super().read(columns).compute()

    def touch(self):
        # Do not run super().touch() because DaskDataFrameClient calls compute()
        # which does not apply here.
        BaseStructureClient.touch(self)
        self._ipython_key_completions_()
        self.read()
