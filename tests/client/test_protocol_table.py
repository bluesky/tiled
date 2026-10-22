"""Unit tests for the table protocol layer (`tiled.client._protocol.table`).

These tests exercise read recipes, write-request builders, data-source
builders, and response parsers as plain functions, with no server and no I/O.
"""

import orjson
import pandas
import pandas.testing
import pyarrow
import pytest

from tiled.client._protocol.request import RequestDraft
from tiled.client._protocol.table import (
    data_source_for_appendable_table,
    data_source_from_table,
    draft_request_append_table_partition,
    draft_request_read_table_partition,
    draft_request_write_table_full,
    draft_request_write_table_partition,
)
from tiled.serialization.table import deserialize_arrow
from tiled.structures.core import StructureFamily
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
def test_draft_request_read_table_partition_get(columns, expected_column_param):
    draft = draft_request_read_table_partition(PARTITION_LINK, 2, columns)
    assert draft.method == "GET"
    assert draft.url == PARTITION_LINK
    # The baked `partition` template in the link's query is overridden by the
    # actual partition index.
    assert draft.params == {"partition": 2, **expected_column_param}
    assert draft.headers == {"Accept": APACHE_ARROW_FILE_MIME_TYPE}
    assert draft.content is None


def test_draft_request_read_table_partition_post(monkeypatch):
    monkeypatch.setattr(RequestDraft, "URL_CHARACTER_LIMIT", 0)
    columns = ["A", "B"]
    draft = draft_request_read_table_partition(PARTITION_LINK, 1, columns)
    assert draft.method == "POST"
    assert draft.url == PARTITION_LINK
    # The columns move to the body; `partition` stays a query parameter.
    assert draft.params == {"partition": 1}
    assert draft.headers == {
        "Accept": APACHE_ARROW_FILE_MIME_TYPE,
        "Content-Type": JSON_MIMETYPE,
    }
    assert draft.content == orjson.dumps(columns)


def test_draft_request_read_table_partition_post_without_columns(monkeypatch):
    # With no column selection there is nothing to move to the body: the
    # fallback POST carries no body and no `Content-Type` header.
    monkeypatch.setattr(RequestDraft, "URL_CHARACTER_LIMIT", 0)
    draft = draft_request_read_table_partition(PARTITION_LINK, 0, None)
    assert draft.method == "POST"
    assert draft.params == {"partition": 0}
    assert draft.headers == {"Accept": APACHE_ARROW_FILE_MIME_TYPE}
    assert draft.content is None


def test_draft_request_write_table_full():
    dataframe = make_dataframe()
    draft = draft_request_write_table_full(FULL_LINK, dataframe)
    assert draft.method == "PUT"
    assert draft.url == FULL_LINK
    assert draft.params == {}
    assert draft.headers == {"Content-Type": APACHE_ARROW_FILE_MIME_TYPE}
    # The body is Arrow IPC file bytes encoding the dataframe.
    pandas.testing.assert_frame_equal(deserialize_arrow(draft.content), dataframe)


@pytest.mark.parametrize(
    "builder, expected_method",
    [
        (draft_request_write_table_partition, "PUT"),
        (draft_request_append_table_partition, "PATCH"),
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


# Data sources submitted when creating table nodes


@pytest.mark.parametrize(
    "data",
    [
        pandas.DataFrame({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]}),
        # A table may also be given as a dict of columns.
        {"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]},
    ],
    ids=["dataframe", "dict"],
)
def test_data_source_from_table(data):
    data_source = data_source_from_table(data)
    assert data_source.structure_family == StructureFamily.table
    assert data_source.mimetype is None
    assert data_source.structure.columns == ["a", "b"]
    assert data_source.structure.npartitions == 1


@pytest.mark.parametrize(
    "table_name, expected_parameters",
    [(None, {}), ("my_table", {"table_name": "my_table"})],
)
def test_data_source_for_appendable_table(table_name, expected_parameters):
    schema = pyarrow.schema([("a", pyarrow.int64()), ("b", pyarrow.float64())])
    data_source = data_source_for_appendable_table(schema, 2, table_name)
    assert data_source.structure_family == StructureFamily.table
    assert data_source.mimetype == "application/x-tiled-sql-table"
    assert data_source.parameters == expected_parameters
    assert data_source.structure.columns == ["a", "b"]
    assert data_source.structure.npartitions == 2
