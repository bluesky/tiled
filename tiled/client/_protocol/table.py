"""Pure request builders and response parsers for the table (dataframe) client.

Each read is one request whose Arrow response decodes with no further
context (`TableReadRecipe`); reading a full table is one request per
partition, orchestrated by the client classes. Writes are plain
`RequestDraft`s (see the package docstring for the convention).

No I/O happens in this module; the client classes send the drafts and feed
response content back through the parsers.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional, Sequence

from ...serialization.table import deserialize_arrow, serialize_arrow
from ...structures.core import StructureFamily
from ...structures.data_source import DataSource
from ...utils import APACHE_ARROW_FILE_MIME_TYPE
from .request import RequestDraft, RetryPolicy, fill_link_template

if TYPE_CHECKING:
    import pandas


@dataclass(frozen=True)
class TableReadRecipe:
    """A recipe for one read of tabular data: the request plus its decoding.

    The Arrow response decodes with no further context, so `parse_response` is a
    staticmethod (and dask tasks can reference it by qualified name without
    embedding the recipe).
    """

    draft: RequestDraft

    @staticmethod
    def parse_response(content: bytes) -> "pandas.DataFrame":
        "Decode an Arrow IPC response body into a pandas DataFrame."
        return deserialize_arrow(content)

    @classmethod
    def for_partition(
        cls,
        link: str,
        partition: int,
        columns: Optional[Sequence[str]],
    ) -> "TableReadRecipe":
        """A recipe that reads one partition, via `/table/partition`.

        Selected columns ride in the URL as a repeated `column` parameter, or in
        the body of an equivalent POST when the URL would be too long (see
        `RequestDraft.get_or_post`).
        """
        # Note: The singular/plural inconsistency here is because ["A", "B"]
        # will be encoded in the URL as column=A&column=B
        draft = RequestDraft.get_or_post(
            link,
            repeated_param="column",
            values=columns,
            params={"partition": partition},
            headers={"Accept": APACHE_ARROW_FILE_MIME_TYPE},
        )
        return cls(draft)

    @classmethod
    def for_wide_table(cls, full_link: str, fields: Sequence[str]) -> "TableReadRecipe":
        """A recipe that reads selected fields of a node as one Arrow table.

        Used by the xarray wide-table optimization: many scalar/short variables
        of one dataset are fetched in a single request against the container's
        `full` link. The selection rides in the URL as a repeated `field` parameter,
        or in the body of an equivalent POST when the URL would be too long (see
        `RequestDraft.get_or_post`); the Arrow encoding is negotiated via the `format` parameter.
        """
        draft = RequestDraft.get_or_post(
            full_link,
            repeated_param="field",
            values=list(fields),
            params={"format": APACHE_ARROW_FILE_MIME_TYPE},
        )
        return cls(draft)


def _arrow_file_body(dataframe: Any) -> bytes:
    "Encode a dataframe as the Arrow IPC file bytes of a write request body."
    return bytes(serialize_arrow(APACHE_ARROW_FILE_MIME_TYPE, dataframe, {}))


def draft_request_write_table_full(link: str, dataframe: Any) -> RequestDraft:
    "Draft the request that overwrites the full table, via `PUT /table/full`."
    return RequestDraft(
        "PUT",
        link,
        headers={"Content-Type": APACHE_ARROW_FILE_MIME_TYPE},
        content=_arrow_file_body(dataframe),
    )


def draft_request_write_table_partition(
    link_template: str, partition: int, dataframe: Any
) -> RequestDraft:
    """Draft the request that overwrites one partition, via `PUT /table/partition`.

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
    """Draft the request that appends rows to one partition, via `PATCH /table/partition`.

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
    """The data source that describes a table upload.

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
    """The data source that describes an appendable (SQL-backed) table.

    `schema` is a `pyarrow.Schema`. `table_name` optionally names the SQL table
    this is stored in; by default the server chooses a name unique to the schema.
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
