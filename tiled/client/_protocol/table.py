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
from ...utils import APACHE_ARROW_FILE_MIME_TYPE
from .request import RequestDraft


def draft_read_partition_request(
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


def _arrow_file_body(dataframe: Any) -> bytes:
    "Encode a dataframe as the Arrow IPC file bytes of a write request body."
    return bytes(serialize_arrow(APACHE_ARROW_FILE_MIME_TYPE, dataframe, {}))


def draft_write_request(link: str, dataframe: Any) -> RequestDraft:
    "Draft a request overwriting the full table, via `PUT /table/full`."
    return RequestDraft(
        "PUT",
        link,
        headers={"Content-Type": APACHE_ARROW_FILE_MIME_TYPE},
        content=_arrow_file_body(dataframe),
    )


def draft_write_partition_request(
    link_template: str, partition: int, dataframe: Any
) -> RequestDraft:
    """Draft a request overwriting one partition, via `PUT /table/partition`.

    The server provides the partition link as a URL template with an `index`
    placeholder, filled in with the partition index.
    """
    url = link_template.format(index=partition)
    return RequestDraft(
        "PUT",
        url,
        headers={"Content-Type": APACHE_ARROW_FILE_MIME_TYPE},
        content=_arrow_file_body(dataframe),
    )


def draft_append_partition_request(
    link_template: str, partition: int, dataframe: Any
) -> RequestDraft:
    """Draft a request appending rows to one partition, via `PATCH /table/partition`.

    The partition link is a URL template, as in `draft_write_partition_request`.
    Appending is not idempotent, so the caller chooses a suitably conservative
    retry policy; the draft itself carries no retry semantics.
    """
    url = link_template.format(index=partition)
    return RequestDraft(
        "PATCH",
        url,
        headers={"Content-Type": APACHE_ARROW_FILE_MIME_TYPE},
        content=_arrow_file_body(dataframe),
    )
