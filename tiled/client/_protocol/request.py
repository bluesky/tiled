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

from dataclasses import dataclass, field
from typing import Any, Iterable, Optional
from urllib.parse import parse_qs, urlparse


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
        Query parameters. When non-empty, they REPLACE the URL's own query
        string (httpx semantics), so builders that add any parameter must
        fold the link's own query into `params` with `merged_query_params`.
    headers : dict
        HTTP headers, including `Content-Type` for requests with a body.
    content : bytes, optional
        The final, already-encoded request body. Serialization is protocol
        knowledge and happens in the builders -- JSON via `safe_json_dump`
        (numpy-safe), Arrow IPC, zipped awkward buffers, raw array bytes --
        which also set the matching `Content-Type` header. This keeps
        builders byte-deterministic and directly unit-testable; shells
        never encode bodies.
    """

    method: str
    url: str
    params: dict[str, Any] = field(default_factory=dict)
    headers: dict[str, str] = field(default_factory=dict)
    content: Optional[bytes] = None


def merged_query_params(link: str, params: Optional[dict] = None) -> dict:
    """Query parameters for a request to a link: the link's own, plus extras.

    Server-provided links may carry query parameters of their own. httpx
    replaces a URL's query string entirely when `params` is passed, so the
    link's own parameters must be folded into the `params` dict to survive.
    Explicitly passed `params` take precedence. Values parsed from the link
    are lists (`parse_qs` form) while passed values keep their type; httpx
    accepts both.
    """
    return {**parse_qs(urlparse(link).query), **(params or {})}


def url_length_for_get_request(url: str, param_name: str, values: Iterable[str]) -> int:
    """Length of a GET request URL with one repeated query parameter.

    Used to decide between a GET with repeated query parameters (e.g.
    `?column=A&column=B`) and an equivalent POST with the values in the
    body, which avoids the URI size limit commonly imposed by HTTP stacks
    (e.g. nginx). Compare the result against the applicable limit
    (`BaseClient.URL_CHARACTER_LIMIT`).
    """
    extra_chars_per_item = len(f"&{param_name}=")
    return len(url) + sum(extra_chars_per_item + len(v) for v in values)


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
    "Endpoint listing the files inside a node's directory assets."
    return self_link.replace("/metadata", "/asset/manifest", 1)


def asset_bytes_link(self_link: str) -> str:
    "Endpoint serving the raw bytes of a node's assets."
    return self_link.replace("/metadata", "/asset/bytes", 1)


def stream_close_link(self_link: str) -> str:
    "Endpoint that closes the node's live update stream."
    return self_link.replace("/metadata/", "/stream/close/", 1)
