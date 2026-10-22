"""Pure request builders for the table (dataframe) client.

The table endpoints serve one partition per request and the responses decode
with `deserialize_arrow` without further context, so reads and writes alike
are described by plain `RequestDraft`s (see the package docstring for the
convention); reading a full table is one request per partition, orchestrated
by the client classes.

No I/O happens in this module; the client classes send the drafts and feed
response content back through the parser functions.
"""

from __future__ import annotations

from typing import Any, Optional, Sequence

from ...serialization.table import serialize_arrow
from ...structures.core import StructureFamily
from ...structures.data_source import DataSource
from ...utils import APACHE_ARROW_FILE_MIME_TYPE
from .request import RequestDraft, RetryPolicy, fill_link_template


def draft_request_read_table_partition(
    link: str,
    partition: int,
    columns: Optional[Sequence[str]],
) -> RequestDraft:
    """Draft the request fetching one partition, via `/table/partition`.

    Selected columns ride in the URL as a repeated `column` parameter, or in
    the body of an equivalent POST when the URL would be too long (see
    `RequestDraft.get_or_post`).
    """
    # Note: The singular/plural inconsistency here is because ["A", "B"]
    # will be encoded in the URL as column=A&column=B
    return RequestDraft.get_or_post(
        link,
        repeated_param="column",
        values=columns,
        params={"partition": partition},
        headers={"Accept": APACHE_ARROW_FILE_MIME_TYPE},
    )


def draft_request_read_wide_table(
    full_link: str, fields: Sequence[str]
) -> RequestDraft:
    """Draft the request fetching selected fields of a node as one Arrow table.

    Used by the xarray wide-table optimization: many scalar/short variables
    of one dataset are fetched in a single request against the container's
    `full` link. The selection rides in the URL as a repeated `field` parameter,
    or in the body of an equivalent POST when the URL would be too long (see
    `RequestDraft.get_or_post`); the Arrow encoding is negotiated via the `format` parameter.
    """
    return RequestDraft.get_or_post(
        full_link,
        repeated_param="field",
        values=list(fields),
        params={"format": APACHE_ARROW_FILE_MIME_TYPE},
    )


def _arrow_file_body(dataframe: Any) -> bytes:
    "Encode a dataframe as the Arrow IPC file bytes of a write request body."
    return bytes(serialize_arrow(APACHE_ARROW_FILE_MIME_TYPE, dataframe, {}))


def draft_request_write_table_full(link: str, dataframe: Any) -> RequestDraft:
    "Draft a request overwriting the full table, via `PUT /table/full`."
    return RequestDraft(
        "PUT",
        link,
        headers={"Content-Type": APACHE_ARROW_FILE_MIME_TYPE},
        content=_arrow_file_body(dataframe),
    )


def draft_request_write_table_partition(
    link_template: str, partition: int, dataframe: Any
) -> RequestDraft:
    """Draft a request overwriting one partition, via `PUT /table/partition`.

    The server provides the partition link as a URL template with an `index`
    placeholder, filled in with the partition index.
    """
    url = fill_link_template(link_template, index=partition)
    return RequestDraft(
        "PUT",
        url,
        headers={"Content-Type": APACHE_ARROW_FILE_MIME_TYPE},
        content=_arrow_file_body(dataframe),
    )


def draft_request_append_table_partition(
    link_template: str, partition: int, dataframe: Any
) -> RequestDraft:
    """Draft a request appending rows to one partition, via `PATCH /table/partition`.

    The partition link is a URL template, as in
    `draft_request_write_table_partition`. Appending is not idempotent: if the
    server processed the request but the response was lost (e.g. a read timeout
    or a 5xx from a proxy), retrying would append the same rows a second time.
    The draft therefore allows retries only on failures that guarantee the
    request was never processed; other errors are raised to the caller, who can
    inspect the table before deciding to re-append.
    """
    url = fill_link_template(link_template, index=partition)
    return RequestDraft(
        "PATCH",
        url,
        headers={"Content-Type": APACHE_ARROW_FILE_MIME_TYPE},
        content=_arrow_file_body(dataframe),
        retry_policy=RetryPolicy.IF_UNPROCESSED,
    )


# The builders below construct the `DataSource` submitted when creating a
# table node. They import pandas/dask-adjacent libraries lazily so that
# importing this module stays light.


def data_source_from_table(data: Any) -> DataSource:
    """The data source describing a table upload.

    `data` may be a dask or pandas DataFrame, or a dict of columns
    (e.g. `{"a": [1, 2, 3], "b": [4, 5, 6]}`).
    """
    import dask.dataframe

    from ...structures.table import TableStructure

    if isinstance(data, dask.dataframe.DataFrame):
        structure = TableStructure.from_dask_dataframe(data)
    elif isinstance(data, dict):
        structure = TableStructure.from_dict(data)
    else:
        structure = TableStructure.from_pandas(data)
    return DataSource(structure=structure, structure_family=StructureFamily.table)


def data_source_for_appendable_table(
    schema: Any, npartitions: int, table_name: Optional[str] = None
) -> DataSource:
    """The data source describing an appendable (SQL-backed) table.

    `schema` is a `pyarrow.Schema`. `table_name` optionally names the SQL
    table this is stored in; by default the server chooses a name unique to
    the schema.
    """
    from ...structures.table import TableStructure

    parameters = {}
    if table_name is not None:
        parameters["table_name"] = table_name
    return DataSource(
        structure=TableStructure.from_schema(schema, npartitions),
        structure_family=StructureFamily.table,
        mimetype="application/x-tiled-sql-table",
        parameters=parameters,
    )
