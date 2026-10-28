"""Request builders for paginated collections, and query-parameter encoding.

Several Tiled endpoints serve collections the same way: `GET` a collection
link with per-collection query parameters, follow the response's
`links.next` to subsequent pages, and report the total in `meta.count`.
`PaginatedCollectionBuilder` drafts the requests for one such collection;
the endpoint link and the parameters that apply to every request against it
are fixed at construction. Consumers include deep search results, composite
contents, and metadata revisions. The page-following loops are I/O-driven
and live in the client classes.

The server supports two pagination schemes: offset (`page[offset]`) and
cursor (`page[cursor]`). The client addresses only the FIRST page itself,
by offset; the server hands back `links.next` in whichever scheme it
prefers (it upgrades to cursors when the backend supports them), and the
client follows that link verbatim -- the draft's query folding preserves
whatever pagination parameters the link carries. The server's `next` links
carry pagination parameters only, so each page's draft re-applies the
non-pagination parameters (encoded queries, sorting, field selection).

A container's children are the richest such collection: their search and
navigation requests carry encoded queries and (per-direction) sorting, so
`ChildrenSearchBuilder` specializes `PaginatedCollectionBuilder` with the
parameter encoding and the container-specific drafts (key lookup within
search results, `/distinct`). `DeepSearchBuilder` is the specialization
for a deep search of a container's descendants, via `/search-deep`.

The query encoding itself (`tiled.queries.queries_to_params`) is shared
client/server knowledge and lives with the query types; the encoded form is
what search-backed collections fix at construction.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional, Sequence

from ...queries import KeyLookup, queries_to_params
from ...type_aliases import JSON
from ..utils import MSGPACK_MIME_TYPE
from .request import RequestDraft, distinct_link


def _reject_pagination_params(params: dict[str, Any]) -> dict[str, Any]:
    """Reject explicitly passed pagination parameters.

    Pagination parameters (`page[offset]`, `page[cursor]`, `page[limit]`)
    belong to the page link itself: the client constructs only the first
    page's link and follows the server's `next` links verbatim. Passing one
    explicitly would override the link (explicit parameters takeprecedence
    in the draft's query folding) and desynchronize the pagination position.
    """
    for key in params:
        if key.startswith("page["):
            raise ValueError(
                f"Pagination parameter {key!r} may not be passed explicitly; "
                "pagination belongs to the page link (see `first_page_link` "
                "and the server-provided `links['next']`)."
            )
    return params


@dataclass(frozen=True)
class PaginatedCollectionBuilder:
    """The request builders for one paginated collection endpoint.

    Attributes
    ----------
    link : str
        The collection endpoint, typically a server-provided link.
    fixed_params : dict
        Query parameters applied to every request against the collection
        (e.g. encoded query filters, or flags such as `select_metadata`).
    """

    link: str
    fixed_params: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _reject_pagination_params(self.fixed_params)

    def first_page_link(self, offset: int, limit: Optional[int] = None) -> str:
        """The link to the first page of the collection, starting at `offset`.

        Each page of results links to the next, so only the first page's link
        is constructed client-side (the server may link subsequent pages by
        offset or by cursor; see the module docstring).
        """
        link = f"{self.link}?page[offset]={offset}"
        if limit is not None:
            link += f"&page[limit]={limit}"
        return link

    def draft_request_read_page(
        self, page_link: str, extra_params: Optional[dict[str, Any]] = None
    ) -> RequestDraft:
        """Draft the request that fetches one page of the collection.

        `page_link` is the first page's link (see `first_page_link`) or a
        server-provided `links["next"]`; its baked pagination query is
        preserved by the draft's query folding. The fixed parameters are
        re-applied to every page.
        """
        return RequestDraft(
            "GET",
            page_link,
            params={
                **self.fixed_params,
                **_reject_pagination_params(extra_params or {}),
            },
            headers={"Accept": MSGPACK_MIME_TYPE},
        )

    def draft_request_count(self) -> RequestDraft:
        "Draft the request that counts the collection's entries, exactly."
        return RequestDraft(
            "GET",
            self.link,
            params={**self.fixed_params, "fields": "count"},
            headers={"Accept": MSGPACK_MIME_TYPE},
        )

    @staticmethod
    def parse_response_count(content: JSON) -> int:
        """Extract the entry count from a collection response.

        NB: in page responses the count may be approximate (the server
        computes an exact count only when `fields=count` is requested, as
        `draft_request_count` does).
        """
        return content["meta"]["count"]


@dataclass(frozen=True)
class DeepSearchBuilder(PaginatedCollectionBuilder):
    """The request builders for a deep search of one container's descendants.

    A deep search (via `/search-deep`) pages like any other collection; only
    its fixed parameters are specific: the encoded queries, an optional
    depth limit, and the data-sources flag.
    """

    @classmethod
    def from_queries(
        cls,
        search_deep_link: str,
        queries: Sequence[Any],
        *,
        max_depth: Optional[int] = None,
        include_data_sources: bool = False,
    ) -> "DeepSearchBuilder":
        "Encode the queries (and options) once, for all drafts built from them."
        fixed_params = queries_to_params(*queries)
        if max_depth is not None:
            fixed_params["max_depth"] = max_depth
        if include_data_sources:
            fixed_params["include_data_sources"] = True
        return cls(link=search_deep_link, fixed_params=fixed_params)


@dataclass(frozen=True)
class ChildrenSearchBuilder(PaginatedCollectionBuilder):
    """The request builders for one container's (filtered) children, via `/search`.

    A container client's queries and sorting are fixed for its lifetime, so
    their encoded query parameters are computed once, at construction, and the
    search/navigation requests are drafted from them. The inherited `link` is
    the container's search link, and the inherited `fixed_params` hold the
    queries encoded as `filter[...]` parameters -- they apply to every
    request. The sorting is held as-is and encoded per draft, because its
    direction can vary (reversed for backwards iteration).

    Attributes
    ----------
    sorting : tuple
        The container's sorting, as (key, direction) tuples.
    """

    sorting: tuple[tuple[str, int], ...] = ()

    @classmethod
    def from_queries(
        cls,
        search_link: str,
        queries: Sequence[Any],
        sorting: Sequence[tuple[str, int]],
    ) -> "ChildrenSearchBuilder":
        "Encode the queries once, for all drafts built from them."
        return cls(
            link=search_link,
            fixed_params=queries_to_params(*queries),
            sorting=tuple(sorting),
        )

    def _directed_sorting_params(self, direction: int) -> dict[str, list[str]]:
        """Encode the sorting as the `sort` query parameter.

        In the Python API sorting is given as (key, direction) tuples; in the
        HTTP API each becomes "key" (ascending) or "-key" (descending). A
        negative `direction` reverses every key's direction, for iterating
        the entries backwards. The server-default sorting encodes to nothing.
        An explicit "+" key prefix means ascending in the HTTP API, the same
        as no prefix; it is normalized away.
        """
        encoded = [
            f"{'-' if key_direction * direction < 0 else ''}{key.removeprefix('+')}"
            for key, key_direction in self.sorting
        ]
        return {"sort": encoded} if "".join(encoded) else {}

    @property
    def sorting_params(self) -> dict[str, list[str]]:
        "The sorting encoded as the `sort` query parameter."
        return self._directed_sorting_params(1)

    @property
    def reversed_sorting_params(self) -> dict[str, list[str]]:
        "The reverse of the sorting, for iterating a node's entries backwards."
        return self._directed_sorting_params(-1)

    @staticmethod
    def may_serve_from_inlined_contents(
        contents: Optional[JSON],
        queries: Sequence[Any],
        sorting: Sequence[tuple[str, int]],
    ) -> bool:
        """Whether inlined contents can serve iteration/slicing without a request.

        The server may send a container's contents in-line (`structure.contents`),
        with the implication that they are not expected to be dynamic. They
        reflect no filtering and the server's default ordering, so they can only
        serve when no queries apply and the sorting is absent or the default.
        """
        return (
            (contents is not None)
            and (not queries)
            and ((not sorting) or (list(sorting) == [("_", 1)]))
        )

    def draft_request_count(self) -> RequestDraft:
        "Draft the request that counts the container's (filtered) entries, exactly."
        return RequestDraft(
            "GET",
            self.link,
            params={**self.fixed_params, "fields": "count", **self.sorting_params},
            headers={"Accept": MSGPACK_MIME_TYPE},
        )

    def draft_request_read_page(
        self,
        page_link: str,
        *,
        keys_only: bool,
        include_data_sources: bool = False,
        direction: int = 1,
    ) -> RequestDraft:
        """Draft the request that fetches one page of the container's entries.

        `page_link` is the first page's link (see `first_page_link`) or a
        server-provided `links["next"]`; its baked pagination query is
        preserved by the draft's query folding. With `keys_only`, no attribute
        fields are requested: only the entry ids are needed. A negative
        `direction` iterates the entries backwards (reversed sorting).
        """
        params = {**self.fixed_params, **self._directed_sorting_params(direction)}
        if keys_only:
            params["fields"] = ""
        if include_data_sources:
            params["include_data_sources"] = True
        return RequestDraft(
            "GET", page_link, params=params, headers={"Accept": MSGPACK_MIME_TYPE}
        )

    def draft_request_lookup_key(
        self, key: str, *, include_data_sources: bool = False
    ) -> RequestDraft:
        """Draft the request that looks up `key` within the search results.

        Used when queries apply to the container: the lookup must only return
        a result if the key is contained in the search results, so it is
        expressed as a `KeyLookup` query alongside the container's own queries.
        """
        params = {
            **queries_to_params(KeyLookup(key)),
            **self.fixed_params,
            **self.sorting_params,
        }
        if include_data_sources:
            params["include_data_sources"] = True
        return RequestDraft(
            "GET", self.link, params=params, headers={"Accept": MSGPACK_MIME_TYPE}
        )

    def draft_request_read_distinct(
        self,
        self_link: str,
        metadata_keys: Sequence[str],
        structure_families: bool,
        specs: bool,
        counts: bool,
    ) -> RequestDraft:
        "Draft the request that fetches distinct metadata values, via `/distinct`."
        return RequestDraft(
            "GET",
            distinct_link(self_link),
            params={
                **self.fixed_params,
                "metadata": metadata_keys,
                "structure_families": structure_families,
                "specs": specs,
                "counts": counts,
            },
            headers={"Accept": MSGPACK_MIME_TYPE},
        )
