"""Unit tests for the pure protocol layer (`tiled.client._protocol`).

These tests exercise request builders, URL/link helpers, and failure
predicates as plain functions, with no server and no I/O.
"""

import dataclasses

import httpx
import orjson
import pytest

from tiled.client._protocol.request import (
    RequestDraft,
    RetryPolicy,
    asset_bytes_link,
    asset_manifest_link,
    distinct_link,
    draft_request_delete,
    draft_request_get_item,
    fill_link_template,
    register_link,
    revisions_link,
    stream_close_link,
)
from tiled.client.utils import (
    ClientError,
    FailureKind,
    classify_failure,
    request_may_have_reached_server,
    should_retry,
    should_retry_if_unprocessed,
)

SELF_LINK = "http://localhost:8000/api/v1/metadata/a/b"


def _http_status_error(status_code, cls=httpx.HTTPStatusError, headers=None):
    request = httpx.Request("GET", "http://example.com/")
    response = httpx.Response(status_code, request=request, headers=headers)
    return cls(f"HTTP {status_code}", request=request, response=response)


def test_request_draft_defaults_and_immutability():
    draft = RequestDraft("GET", "http://example.com/x")
    assert draft.params == {}
    assert draft.headers == {}
    assert draft.content is None
    assert draft.retry_policy is RetryPolicy.IDEMPOTENT
    with pytest.raises(dataclasses.FrozenInstanceError):
        draft.method = "POST"


def test_retry_policy_declared_by_builders():
    # Appending rows is not idempotent; the builder declares the
    # conservative policy so every sending shell honors it.
    import pandas

    from tiled.client._protocol.table import draft_request_append_table_partition

    draft = draft_request_append_table_partition(
        "http://example.com/api/v1/table/partition/x?partition={index}",
        0,
        pandas.DataFrame({"a": [1]}),
    )
    assert draft.retry_policy is RetryPolicy.IF_UNPROCESSED


@pytest.mark.parametrize(
    "url, params, expected",
    [
        # A URL without a query contributes nothing.
        ("http://example.com/x", {}, {}),
        ("http://example.com/x", {"a": 1}, {"a": 1}),
        # The URL's own query parameters are folded in (parse_qs form).
        (
            "http://example.com/x?baked=1&baked=2&other=z",
            {"a": 1},
            {"baked": ["1", "2"], "other": ["z"], "a": 1},
        ),
        # Explicitly passed parameters take precedence over baked ones.
        ("http://example.com/x?key=baked", {"key": "explicit"}, {"key": "explicit"}),
        # The folding is what keeps baked queries alive: httpx strips a
        # URL's query whenever `params` is not None, even an empty dict.
        ("http://example.com/x?partition=3", {}, {"partition": ["3"]}),
    ],
)
def test_request_draft_folds_link_query_automatically(url, params, expected):
    assert RequestDraft("GET", url, params=params).params == expected


def test_get_or_post_threshold(monkeypatch):
    # The URL length that decides GET vs POST counts the URL plus each
    # repeated `&column=<value>` pair; a URL exactly at the limit is still
    # sent as a GET, one character over falls back to POST.
    url = "http://example.com/table/partition/x"
    columns = ["A", "longer_name"]
    url_length = len(url) + (len("&column=") + 1) + (len("&column=") + 11)
    monkeypatch.setattr(RequestDraft, "URL_CHARACTER_LIMIT", url_length)
    draft = RequestDraft.get_or_post(url, repeated_param="column", values=columns)
    assert draft.method == "GET"
    assert draft.params == {"column": columns}
    monkeypatch.setattr(RequestDraft, "URL_CHARACTER_LIMIT", url_length - 1)
    draft = RequestDraft.get_or_post(url, repeated_param="column", values=columns)
    assert draft.method == "POST"
    assert draft.content == orjson.dumps(columns)
    assert draft.headers["Content-Type"] == "application/json"


def test_get_or_post_without_values(monkeypatch):
    # `values=None` means no selection; a fallback POST carries no body and
    # no `Content-Type` header.
    url = "http://example.com/table/partition/x"
    monkeypatch.setattr(RequestDraft, "URL_CHARACTER_LIMIT", 0)
    draft = RequestDraft.get_or_post(url, repeated_param="column", values=None)
    assert draft.method == "POST"
    assert draft.content is None
    assert "Content-Type" not in draft.headers


@pytest.mark.parametrize(
    "helper, expected",
    [
        (distinct_link, "http://localhost:8000/api/v1/distinct/a/b"),
        (register_link, "http://localhost:8000/api/v1/register/a/b"),
        (revisions_link, "http://localhost:8000/api/v1/revisions/a/b"),
        (asset_manifest_link, "http://localhost:8000/api/v1/asset/manifest/a/b"),
        (asset_bytes_link, "http://localhost:8000/api/v1/asset/bytes/a/b"),
        (stream_close_link, "http://localhost:8000/api/v1/stream/close/a/b"),
    ],
)
def test_link_derivation(helper, expected):
    assert helper(SELF_LINK) == expected


def test_link_derivation_replaces_first_occurrence_only():
    # A node key may itself contain the substring being replaced; only the
    # route segment (the first occurrence) is rewritten.
    link = "http://localhost:8000/api/v1/metadata/a/metadata"
    assert distinct_link(link) == "http://localhost:8000/api/v1/distinct/a/metadata"
    link = "http://localhost:8000/api/v1/metadata/a/stream/close/"
    assert (
        stream_close_link(link)
        == "http://localhost:8000/api/v1/stream/close/a/stream/close/"
    )


# One matrix covering the failure taxonomy and all three predicates built on
# it. Columns: expected FailureKind, should_retry, should_retry_if_unprocessed,
# request_may_have_reached_server.
@pytest.mark.parametrize(
    "exception, kind, retry, retry_if_unprocessed, reached",
    [
        # Connect-phase transport errors: the request never left the client.
        (httpx.ConnectError("x"), FailureKind.NEVER_SENT, True, True, False),
        (httpx.ConnectTimeout("x"), FailureKind.NEVER_SENT, True, True, False),
        (httpx.PoolTimeout("x"), FailureKind.NEVER_SENT, True, True, False),
        # Read/write-phase transport errors: the request may have been
        # received and processed even though the response was lost.
        (httpx.ReadTimeout("x"), FailureKind.RESPONSE_LOST, True, False, True),
        (httpx.ReadError("x"), FailureKind.RESPONSE_LOST, True, False, True),
        (httpx.WriteTimeout("x"), FailureKind.RESPONSE_LOST, True, False, True),
        (httpx.WriteError("x"), FailureKind.RESPONSE_LOST, True, False, True),
        (httpx.RemoteProtocolError("x"), FailureKind.RESPONSE_LOST, True, False, True),
        # 429: explicitly refused without processing; both policies retry.
        (_http_status_error(429), FailureKind.REFUSED, True, True, False),
        # 5xx: the server/an intermediary may have processed the request.
        (_http_status_error(500), FailureKind.SERVER_ERROR, True, False, True),
        (_http_status_error(502), FailureKind.SERVER_ERROR, True, False, True),
        (_http_status_error(503), FailureKind.SERVER_ERROR, True, False, True),
        # ClientError subclasses httpx.HTTPStatusError and classifies by status.
        (
            _http_status_error(502, cls=ClientError),
            FailureKind.SERVER_ERROR,
            True,
            False,
            True,
        ),
        (
            _http_status_error(409, cls=ClientError),
            FailureKind.REJECTED,
            False,
            False,
            False,
        ),
        # Other 4xx: processed and rejected; retrying cannot succeed.
        (_http_status_error(400), FailureKind.REJECTED, False, False, False),
        (_http_status_error(404), FailureKind.REJECTED, False, False, False),
        # Requests that can never succeed.
        (httpx.UnsupportedProtocol("x"), FailureKind.MALFORMED, False, False, False),
        (httpx.LocalProtocolError("x"), FailureKind.MALFORMED, False, False, False),
        # Remaining httpx errors: the default policy retries them, the
        # non-idempotent policy does not.
        (httpx.ProxyError("x"), FailureKind.OTHER, True, False, False),
        (httpx.CloseError("x"), FailureKind.OTHER, True, False, False),
        (httpx.DecodingError("x"), FailureKind.OTHER, True, False, False),
        # Failures unrelated to HTTP are never retried.
        (ValueError("x"), FailureKind.OTHER, False, False, False),
    ],
)
def test_failure_taxonomy(exception, kind, retry, retry_if_unprocessed, reached):
    assert classify_failure(exception) is kind
    assert should_retry(exception) is retry
    assert should_retry_if_unprocessed(exception) is retry_if_unprocessed
    assert request_may_have_reached_server(exception) is reached


def test_retry_after_header_sets_wait_time():
    # A numeric Retry-After on a 429 becomes the wait time for both retry
    # policies; a non-numeric one falls back to the default backoff (True).
    exc = _http_status_error(429, headers={"Retry-After": "7"})
    assert should_retry(exc) == 7.0
    assert should_retry_if_unprocessed(exc) == 7.0
    exc = _http_status_error(
        429, headers={"Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT"}
    )
    assert should_retry(exc) is True
    assert should_retry_if_unprocessed(exc) is True


@pytest.mark.parametrize(
    "template, args, kwargs, expected",
    [
        ("http://x/block/a?block={0},{1}", (1, 0), {}, "http://x/block/a?block=1,0"),
        (
            "http://x/partition/a?partition={index}",
            (),
            {"index": 2},
            "http://x/partition/a?partition=2",
        ),
        # A 0-dimensional block legitimately has no placeholders.
        ("http://x/block/a?block=", (), {}, "http://x/block/a?block="),
    ],
)
def test_fill_link_template(template, args, kwargs, expected):
    assert fill_link_template(template, *args, **kwargs) == expected


@pytest.mark.parametrize(
    "template, args, kwargs",
    [
        # A link without the expected placeholders would otherwise be used
        # unchanged, silently addressing the wrong resource.
        ("http://x/full/a", (1, 0), {}),
        ("http://x/full/a", (), {"index": 2}),
        # Too few arguments for the template's placeholders.
        ("http://x/block/a?block={0},{1}", (1,), {}),
        # Named/positional mismatch.
        ("http://x/partition/a?partition={index}", (2,), {}),
    ],
)
def test_fill_link_template_mismatch_raises(template, args, kwargs):
    with pytest.raises(ValueError):
        fill_link_template(template, *args, **kwargs)


def test_draft_request_get_item():
    draft = draft_request_get_item(SELF_LINK, {"fields": "structure"})
    assert draft.method == "GET"
    assert draft.url == SELF_LINK
    assert draft.params == {"fields": "structure"}
    assert draft.headers == {"Accept": "application/x-msgpack"}


def test_draft_request_delete():
    draft = draft_request_delete(SELF_LINK + "?a=1", {"number": 3})
    assert draft.method == "DELETE"
    # The link's own query participates alongside the passed parameters.
    assert draft.params == {"a": ["1"], "number": 3}
    assert draft.headers == {}
    assert draft.content is None
