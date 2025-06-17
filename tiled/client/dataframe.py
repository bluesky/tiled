import functools
from urllib.parse import parse_qs, urlparse

import dask
import dask.dataframe
import httpx

from ..serialization.table import deserialize_arrow, serialize_arrow
from ..utils import APACHE_ARROW_FILE_MIME_TYPE, UNCHANGED
from .base import BaseClient
from .utils import (
    MSGPACK_MIME_TYPE,
    ClientError,
    client_for_item,
    export_util,
    handle_error,
    retry_context,
)

_EXTRA_CHARS_PER_ITEM = len("&column=")


class _DaskDataFrameClient(BaseClient):
    "Client-side wrapper around an dataframe-like that returns dask dataframes"

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
        if not structure.resizable:
            p.text(f"<{type(self).__name__} {structure.columns}>")
        else:
            # Try to get the column names, but give up quickly to avoid blocking
            # for long.
            TIMEOUT = 0.2  # seconds
            try:
                content = handle_error(
                    self.context.http_client.get(
                        self.uri,
                        headers={"Accept": MSGPACK_MIME_TYPE},
                        params={
                            **parse_qs(urlparse(self.uri).query),
                            "fields": "structure",
                        },
                        timeout=TIMEOUT,
                    )
                ).json()
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
                    columns = content["data"]["attributes"]["structure"]["columns"]
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
        if not structure.resizable:
            # Use cached structure.
            return structure.columns
        try:
            content = handle_error(
                self.context.http_client.get(
                    self.uri,
                    headers={"Accept": MSGPACK_MIME_TYPE},
                    params={
                        **parse_qs(urlparse(self.uri).query),
                        "fields": "structure",
                    },
                )
            ).json()
            columns = content["data"]["attributes"]["structure"]["columns"]
        except Exception:
            # Do not print messy traceback from thread. Just fail silently.
            return []
        return columns

    @property
    def columns(self):
        return self.structure().columns

    def _get_partition(self, partition, columns):
        """
        Fetch the actual data for one partition in a partitioned (dask) dataframe.

        See read_partition for a public version of this.
        """
        URL_PATH = self.item["links"]["partition"]
        params = {**parse_qs(urlparse(URL_PATH).query), "partition": partition}
        url_length_for_get_request = len(URL_PATH) + sum(
            _EXTRA_CHARS_PER_ITEM + len(column) for column in (columns or ())
        )
        if url_length_for_get_request > self.URL_CHARACTER_LIMIT:
            for attempt in retry_context():
                with attempt:
                    content = handle_error(
                        self.context.http_client.post(
                            URL_PATH,
                            headers={"Accept": APACHE_ARROW_FILE_MIME_TYPE},
                            json=columns,
                            params=params,
                        )
                    ).read()
        else:
            if columns:
                # Note: The singular/plural inconsistency here is because
                # ["A", "B"] will be encoded in the URL as column=A&column=B
                params["column"] = columns
            for attempt in retry_context():
                with attempt:
                    content = handle_error(
                        self.context.http_client.get(
                            URL_PATH,
                            headers={"Accept": APACHE_ARROW_FILE_MIME_TYPE},
                            params=params,
                        )
                    ).read()
        return deserialize_arrow(content)

    def read_partition(self, partition, columns=None):
        """
        Access one partition in a partitioned (dask) dataframe.

        Optionally select a subset of the columns.
        """
        structure = self.structure()
        npartitions = structure.npartitions
        if not (0 <= partition < npartitions):
            raise IndexError(f"partition {partition} out of range")
        meta = structure.meta
        if columns is not None:
            meta = meta[columns]
        return dask.dataframe.from_delayed(
            [dask.delayed(self._get_partition)(partition, columns)],
            meta=meta,
            divisions=(None,) * (1 + npartitions),
        )

    def read(self, columns=None):
        """
        Access the entire DataFrame. Optionally select a subset of the columns.

        The result will be internally partitioned with dask.
        """
        structure = self.structure()
        # Build a client-side dask dataframe whose partitions pull from a
        # server-side dask array.
        label = f"remote-dask-dataframe-{self.item['links']['self']}"
        meta = structure.meta
        if columns is not None:
            meta = meta[columns]

        ddf = dask.dataframe.from_map(
            functools.partial(self._get_partition, columns=columns),
            range(structure.npartitions),
            meta=meta,
            label=label,
            divisions=(None,) * (1 + structure.npartitions),
        )

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
            for attempt in retry_context():
                with attempt:
                    content = handle_error(
                        self.context.http_client.get(
                            self_link + f"/{column}",
                            headers={"Accept": MSGPACK_MIME_TYPE},
                        )
                    ).json()
        except ClientError as err:
            if err.response.status_code == httpx.codes.NOT_FOUND:
                raise KeyError(column)
            raise
        item = content["data"]
        return client_for_item(self.context, self.structure_clients, item)

    def __iter__(self):
        yield from self.structure().columns

    # __len__ is intentionally not implemented. For DataFrames it means "number
    # of rows" which is expensive to compute.

    def write(self, dataframe):
        for attempt in retry_context():
            with attempt:
                handle_error(
                    self.context.http_client.put(
                        self.item["links"]["full"],
                        content=bytes(
                            serialize_arrow(APACHE_ARROW_FILE_MIME_TYPE, dataframe, {})
                        ),
                        headers={"Content-Type": APACHE_ARROW_FILE_MIME_TYPE},
                    )
                )

    def write_partition(self, dataframe, partition):
        for attempt in retry_context():
            with attempt:
                handle_error(
                    self.context.http_client.put(
                        self.item["links"]["partition"].format(index=partition),
                        content=bytes(
                            serialize_arrow(APACHE_ARROW_FILE_MIME_TYPE, dataframe, {})
                        ),
                        headers={"Content-Type": APACHE_ARROW_FILE_MIME_TYPE},
                    )
                )

    def append_partition(self, dataframe, partition):
        if partition > self.structure().npartitions:
            raise ValueError(f"Table has {self.structure().npartitions} partitions")
        for attempt in retry_context():
            with attempt:
                handle_error(
                    self.context.http_client.patch(
                        self.item["links"]["partition"].format(index=partition),
                        content=bytes(
                            serialize_arrow(APACHE_ARROW_FILE_MIME_TYPE, dataframe, {})
                        ),
                        headers={"Content-Type": APACHE_ARROW_FILE_MIME_TYPE},
                    )
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
            params["column"] = columns
        return export_util(
            filepath,
            format,
            self.context.http_client.get,
            self.item["links"]["full"],
            params=params,
        )


# Subclass with a public class that adds the dask-specific methods.


class DaskDataFrameClient(_DaskDataFrameClient):
    "Client-side wrapper around an dataframe-like that returns dask dataframes"

    def compute(self):
        "Alias to client.read().compute()"
        return self.read().compute()


class DataFrameClient(_DaskDataFrameClient):
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
