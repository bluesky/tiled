"""The `RequestDraft` dataclass and pure URL/link helpers.

Request builders in this package return `RequestDraft`s -- declarative
descriptions of HTTP requests -- and the link helpers encode how the Tiled
API derives related endpoints from a node's self link. The shells are
responsible for turning drafts into actual I/O.

The link helpers are purely syntactic: deriving a link does not imply that
the endpoint is meaningful for a given node (e.g. asset endpoints on a node
without data sources, or the stream-close endpoint on a server without
streaming configured). The server answers such requests with an error
response (404 and the like), which the caller handles.
"""

from __future__ import annotations

import enum
import string
from dataclasses import dataclass, field
from typing import Any, ClassVar, Optional, Sequence
from urllib.parse import parse_qs, urlparse

import orjson

from ...mimetypes import JSON_MIMETYPE, MSGPACK_MIMETYPE


class RetryPolicy(enum.Enum):
    """How the sending shell may retry a request that fails.

    The policy is wire-level knowledge -- whether repeating the request is
    safe follows from what the request does on the server -- so builders
    declare it on the draft, and the shells' send loop acts on it.

    IDEMPOTENT
        Repeating the request is always safe. Retried on any failure where
        a retry could plausibly succeed (the `should_retry` predicate).
    IF_UNPROCESSED
        Repeating is safe only if the server never processed the request
        (e.g. appending rows); retried only on failures that guarantee
        non-processing (the `should_retry_if_unprocessed` predicate).
    NONE
        Single attempt; the caller orchestrates any retries itself (e.g.
        the create flow, which tracks state across attempts).
    """

    IDEMPOTENT = "idempotent"
    IF_UNPROCESSED = "if_unprocessed"
    NONE = "none"


@dataclass(frozen=True)
class RequestDraft:
    """A draft of one HTTP request: fully determined, but not built or sent.

    Attributes
    ----------
    method : str
        HTTP method, e.g. "GET".
    url : str
        An absolute URL, typically a server-provided link
        (`item["links"][...]`). It may itself carry a query string; see
        `params`.
    params : dict
        Query parameters. The URL's own query is folded in automatically at
        construction, with explicitly passed parameters taking precedence.
        The folding matters because shells pass `params` to httpx
        unconditionally, and httpx REPLACES the URL's query string whenever
        `params` is not None -- even an empty dict strips it.
    headers : dict
        HTTP headers, including `Content-Type` for requests with a body.
    content : bytes, optional
        The final, already-encoded request body. Serialization is protocol
        knowledge and happens in the builders -- JSON via `safe_json_dump`
        (numpy-safe), Arrow IPC, zipped awkward buffers, raw array bytes --
        which also set the matching `Content-Type` header. This keeps
        builders byte-deterministic and directly unit-testable; shells
        never encode bodies.
    retry_policy : RetryPolicy
        Whether and when the sending shell may retry this request.
    """

    # The longest URL the client sends in a GET request, in characters. A wire-level
    # policy for the whole client: HTTP stacks (e.g. nginx) commonly impose a URI size limit.
    URL_CHARACTER_LIMIT: ClassVar[int] = 2_000

    method: str
    url: str
    params: dict[str, Any] = field(default_factory=dict)
    headers: dict[str, str] = field(default_factory=dict)
    content: Optional[bytes] = None
    retry_policy: RetryPolicy = RetryPolicy.IDEMPOTENT

    @classmethod
    def get_or_post(
        cls,
        url: str,
        *,
        repeated_param: str,
        values: Optional[Sequence[str]],
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
    ) -> "RequestDraft":
        """A GET with one repeated query parameter, or an equivalent POST.

        Tiled endpoints that select a subset via a repeated query parameter
        (e.g. `?column=A&column=B`) accept, interchangeably, a POST with the
        selection as a JSON list in the body. The POST form is used when the
        GET URL would exceed `URL_CHARACTER_LIMIT`. `values=None` means no
        selection (the endpoint's default); a fallback POST then carries no
        body.

        Exactly one parameter participates in the fallback: the body is an
        unkeyed JSON list, so it can carry only one parameter's values. Other
        repeated parameters may be passed via `params`; they stay in the URL.
        """
        extra_chars_per_item = len(f"&{repeated_param}=")
        url_length = len(url) + sum(
            extra_chars_per_item + len(value) for value in values or ()
        )
        if url_length > cls.URL_CHARACTER_LIMIT:
            content = None
            if values is not None:
                content = orjson.dumps(list(values))
                headers = {**(headers or {}), "Content-Type": JSON_MIMETYPE}
            return cls(
                "POST", url, params=params or {}, headers=headers or {}, content=content
            )
        params = dict(params or {})
        if values:
            params[repeated_param] = values
        return cls("GET", url, params=params, headers=headers or {})

    def __post_init__(self) -> None:
        # Fold the URL's own query into `params` (see the class docstring).
        # Values parsed from the link are lists (`parse_qs` form) while
        # passed values keep their type; httpx accepts both. The dataclass
        # is frozen, so assign through `object.__setattr__`.
        object.__setattr__(
            self, "params", {**parse_qs(urlparse(self.url).query), **self.params}
        )


def draft_request_get_item(
    link: str, params: Optional[dict[str, Any]] = None
) -> RequestDraft:
    """Draft a GET of one item (msgpack-encoded JSON on the wire).

    The generic shape of single-item reads in the Tiled API: the item
    describing a node, a metadata revision, and the like. Operations with
    more to say have named builders that delegate here.
    """
    return RequestDraft(
        "GET",
        link,
        params=params or {},
        headers={"Accept": MSGPACK_MIMETYPE},
    )


def draft_request_delete(
    link: str, params: Optional[dict[str, Any]] = None
) -> RequestDraft:
    """Draft a DELETE of the resource a link addresses.

    The generic shape of all deletions in the Tiled API: nothing is parsed
    from the response; parameters select or scope what is deleted (e.g. a
    revision `number`, or `recursive` for a node's contents).
    """
    return RequestDraft("DELETE", link, params=params or {})


def fill_link_template(template: str, *args: Any, **kwargs: Any) -> str:
    """Fill a server-provided URL template, validating its placeholders.

    Some server links are URL templates (e.g. `...?block={0},{1}` or
    `...?partition={index}`). `str.format` silently ignores surplus
    arguments, so a link without the expected placeholders would be used
    unchanged -- addressing the wrong resource. The placeholders present in
    the template must exactly account for the arguments provided.
    """
    fields = [
        name for _, name, _, _ in string.Formatter().parse(template) if name is not None
    ]
    positional = sum(1 for name in fields if name == "" or name.isdigit())
    named = {name for name in fields if name and not name.isdigit()}
    if positional != len(args) or named != set(kwargs):
        raise ValueError(
            f"Link template {template!r} does not match the expected "
            f"placeholders: {len(args)} positional and {sorted(kwargs)} named."
        )
    return template.format(*args, **kwargs)


# The Tiled API exposes several facets of one node on sibling endpoints.
# Each helper below derives a facet's endpoint from the node's self link
# (`item["links"]["self"]`, which points at `.../metadata/{path}`).


def distinct_link(self_link: str) -> str:
    "Endpoint serving the distinct metadata keys/values of a node's children."
    return self_link.replace("/metadata", "/distinct", 1)


def register_link(self_link: str) -> str:
    "Endpoint for creating a child node that references existing assets."
    return self_link.replace("/metadata/", "/register/", 1)


def revisions_link(self_link: str) -> str:
    "Endpoint serving a node's metadata revision history."
    return self_link.replace("/metadata", "/revisions", 1)


def asset_manifest_link(self_link: str) -> str:
    "Endpoint serving the manifests of a node's assets."
    return self_link.replace("/metadata", "/asset/manifest", 1)


def asset_bytes_link(self_link: str) -> str:
    "Endpoint serving the raw bytes of a node's assets."
    return self_link.replace("/metadata", "/asset/bytes", 1)


def stream_close_link(self_link: str) -> str:
    "Endpoint that closes the node's live update stream."
    return self_link.replace("/metadata/", "/stream/close/", 1)
