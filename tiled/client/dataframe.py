import dask
import dask.dataframe

from ..serialization.dataframe import deserialize_arrow, serialize_arrow
from ..utils import APACHE_ARROW_FILE_MIME_TYPE, UNCHANGED
from .base import BaseStructureClient
from .utils import ClientError, client_for_item, export_util


class DaskDataFrameClient(BaseStructureClient):
    "Client-side wrapper around an array-like that returns dask arrays"

    def new_variation(self, structure=UNCHANGED, **kwargs):
        if structure is UNCHANGED:
            structure = self._structure
        return super().new_variation(structure=structure, **kwargs)

    def _repr_pretty_(self, p, cycle):
        """
        Provide "pretty" display in IPython/Jupyter.

        See https://ipython.readthedocs.io/en/stable/config/integrating.html#rich-display
        """
        structure = self.structure()
        if not structure.macro.resizable:
            p.text(f"<{type(self).__name__} {structure.macro.columns}>")
        else:
            # Try to get the column names, but give up quickly to avoid blocking
            # for long.
            TIMEOUT = 0.2  # seconds
            try:
                content = self.context.get_json(
                    self.uri,
                    params={"fields": "structure.macro"},
                    timeout=TIMEOUT,
                )
            except TimeoutError:
                p.text(
                    f"<{type(self).__name__} Loading column names took too long; use list(...) >"
                )
            except Exception as err:
                p.text(
                    f"<{type(self).__name__} Loading column names raised error {err!r}>"
                )
            else:
                try:
                    columns = content["data"]["attributes"]["structure"]["macro"][
                        "columns"
                    ]
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
        structure = self.structure()
        if not structure.macro.resizable:
            # Use cached structure.
            return structure.macro.columns
        try:
            content = self.context.get_json(
                self.uri, params={"fields": "structure.macro"}
            )
            columns = content["data"]["attributes"]["structure"]["macro"]["columns"]
        except Exception:
            # Do not print messy traceback from thread. Just fail silently.
            return []
        return columns

    @property
    def columns(self):
        return self.structure().macro.columns

    def download(self):
        super().download()
        self._ipython_key_completions_()
        self.read().compute()

    def _get_partition(self, partition, columns):
        """
        Fetch the actual data for one partition in a partitioned (dask) dataframe.

        See read_partition for a public version of this.
        """
        params = {"partition": partition}
        if columns:
            # Note: The singular/plural inconsistency here is due to the fact that
            # ["A", "B"] will be encoded in the URL as field=A&field=B
            params["field"] = columns
        full_path = (
            "/dataframe/partition"
            + "".join(f"/{path}" for path in self.context.path_parts)
            + "".join(f"/{path}" for path in self._path)
        )
        content = self.context.get_content(
            full_path,
            headers={"Accept": APACHE_ARROW_FILE_MIME_TYPE},
            params=params,
        )
        return deserialize_arrow(content)

    def read_partition(self, partition, columns=None):
        """
        Access one partition in a partitioned (dask) dataframe.

        Optionally select a subset of the columns.
        """
        structure = self.structure()
        npartitions = structure.macro.npartitions
        if not (0 <= partition < npartitions):
            raise IndexError(f"partition {partition} out of range")
        meta = structure.micro.meta_decoded
        if columns is not None:
            meta = meta[columns]
        return dask.dataframe.from_delayed(
            [dask.delayed(self._get_partition)(partition, columns)],
            meta=meta,
            divisions=structure.micro.divisions_decoded,
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
            "remote-dask-dataframe-" f"{self.context.base_url!s}/{'/'.join(self._path)}"
        )
        dask_tasks = {
            (name,) + (partition,): (self._get_partition, partition, columns)
            for partition in range(structure.macro.npartitions)
        }
        meta = structure.micro.meta_decoded

        if columns is not None:
            meta = meta[columns]
        ddf = dask.dataframe.DataFrame(
            dask_tasks,
            name=name,
            meta=meta,
            divisions=structure.micro.divisions_decoded,
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

    def __getitem__(self, column):
        try:
            self_link = self.item["links"]["self"]
            if self_link.endswith("/"):
                self_link = self_link[:-1]
            content = self.context.get_json(
                self_link + f"/{column}",
            )
        except ClientError as err:
            if err.response.status_code == 404:
                raise KeyError(column)
            raise
        item = content["data"]
        return client_for_item(
            self.context, self.structure_clients, item, path=self._path + (item["id"],)
        )

    def __iter__(self):
        yield from self.structure().macro.columns

    # __len__ is intentionally not implemented. For DataFrames it means "number
    # of rows" which is expensive to compute.

    def write(self, dataframe):
        self.context.put_content(
            self.item["links"]["full"],
            content=bytes(serialize_arrow(dataframe, {})),
            headers={"Content-Type": APACHE_ARROW_FILE_MIME_TYPE},
        )

    def write_partition(self, dataframe, partition):
        self.context.put_content(
            self.item["links"]["partition"].format(index=partition),
            content=bytes(serialize_arrow(dataframe, {})),
            headers={"Content-Type": APACHE_ARROW_FILE_MIME_TYPE},
        )

    def export(self, filepath, columns=None, *, format=None):
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
            params["field"] = columns
        return export_util(
            filepath,
            format,
            self.context.get_content,
            self.item["links"]["full"],
            params=params,
        )


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
        return super().read(columns).compute()

    def download(self):
        # Do not run super().download() because DaskDataFrameClient calls compute()
        # which does not apply here.
        BaseStructureClient.download(self)
        self._ipython_key_completions_()
        self.read()
