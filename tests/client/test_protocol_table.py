"""Unit tests for the table protocol layer (`tiled.client._protocol.table`).

These tests exercise read recipes, write-request builders, and response
parsers as plain functions, with no server and no I/O.
"""

import orjson
import pandas
import pandas.testing
import pytest

from tiled.client._protocol.request import RequestDraft
from tiled.client._protocol.table import (
    draft_append_partition_request,
    draft_read_partition_request,
    draft_write_partition_request,
    draft_write_request,
)
from tiled.serialization.table import deserialize_arrow
from tiled.utils import APACHE_ARROW_FILE_MIME_TYPE

FULL_LINK = "http://localhost:8000/api/v1/table/full/a/b"
# The server provides the partition link with the `partition` query parameter
# as a URL template with an `index` placeholder.
PARTITION_LINK = "http://localhost:8000/api/v1/table/partition/a/b?partition={index}"
JSON_MIMETYPE = "application/json"


def make_dataframe(start=0, stop=3):
    index = range(start, stop)
    return pandas.DataFrame({"A": [i * 10 for i in index], "B": list(index)}, index)


@pytest.mark.parametrize(
    "columns, expected_column_param",
    [
        # Selected columns ride in the URL as a repeated `column` parameter.
        (["A", "B"], {"column": ["A", "B"]}),
        # With no selection, no `column` parameter is sent.
        (None, {}),
    ],
)
def test_draft_read_partition_request_get(columns, expected_column_param):
    draft = draft_read_partition_request(PARTITION_LINK, 2, columns)
    assert draft.method == "GET"
    assert draft.url == PARTITION_LINK
    # The baked `partition` template in the link's query is overridden by the
    # actual partition index.
    assert draft.params == {"partition": 2, **expected_column_param}
    assert draft.headers == {"Accept": APACHE_ARROW_FILE_MIME_TYPE}
    assert draft.content is None


@pytest.mark.parametrize(
    "limit_delta, expected_method",
    [
        # A URL exactly at the limit still fits in a GET request.
        (0, "GET"),
        # One character over the limit falls back to POST.
        (-1, "POST"),
    ],
)
def test_draft_read_partition_request_url_limit_threshold(
    monkeypatch, limit_delta, expected_method
):
    columns = ["A", "B"]
    url_length = len(PARTITION_LINK) + sum(
        len("&column=") + len(column) for column in columns
    )
    monkeypatch.setattr(RequestDraft, "URL_CHARACTER_LIMIT", url_length + limit_delta)
    draft = draft_read_partition_request(PARTITION_LINK, 0, columns)
    assert draft.method == expected_method


def test_draft_read_partition_request_post(monkeypatch):
    monkeypatch.setattr(RequestDraft, "URL_CHARACTER_LIMIT", 0)
    columns = ["A", "B"]
    draft = draft_read_partition_request(PARTITION_LINK, 1, columns)
    assert draft.method == "POST"
    assert draft.url == PARTITION_LINK
    # The columns move to the body; `partition` stays a query parameter.
    assert draft.params == {"partition": 1}
    assert draft.headers == {
        "Accept": APACHE_ARROW_FILE_MIME_TYPE,
        "Content-Type": JSON_MIMETYPE,
    }
    assert draft.content == orjson.dumps(columns)


def test_draft_read_partition_request_post_without_columns(monkeypatch):
    # With no column selection there is nothing to move to the body: the
    # fallback POST carries no body and no `Content-Type` header.
    monkeypatch.setattr(RequestDraft, "URL_CHARACTER_LIMIT", 0)
    draft = draft_read_partition_request(PARTITION_LINK, 0, None)
    assert draft.method == "POST"
    assert draft.params == {"partition": 0}
    assert draft.headers == {"Accept": APACHE_ARROW_FILE_MIME_TYPE}
    assert draft.content is None


def test_draft_write_request():
    dataframe = make_dataframe()
    draft = draft_write_request(FULL_LINK, dataframe)
    assert draft.method == "PUT"
    assert draft.url == FULL_LINK
    assert draft.params == {}
    assert draft.headers == {"Content-Type": APACHE_ARROW_FILE_MIME_TYPE}
    # The body is Arrow IPC file bytes encoding the dataframe.
    pandas.testing.assert_frame_equal(deserialize_arrow(draft.content), dataframe)


@pytest.mark.parametrize(
    "builder, expected_method",
    [
        (draft_write_partition_request, "PUT"),
        (draft_append_partition_request, "PATCH"),
    ],
)
def test_draft_write_and_append_partition_requests(builder, expected_method):
    dataframe = make_dataframe()
    draft = builder(PARTITION_LINK, 2, dataframe)
    # The partition index fills the URL template and is folded back into params.
    assert draft.method == expected_method
    assert draft.url == PARTITION_LINK.format(index=2)
    assert draft.params == {"partition": ["2"]}
    assert draft.headers == {"Content-Type": APACHE_ARROW_FILE_MIME_TYPE}
    pandas.testing.assert_frame_equal(deserialize_arrow(draft.content), dataframe)
