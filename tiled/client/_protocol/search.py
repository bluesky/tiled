"""Request builders for paginated collections, and query-parameter encoding.

Several Tiled endpoints serve collections the same way: `GET` a collection
link with per-collection query parameters, page with `page[offset]` /
`page[limit]`, follow the response's `links.next` to subsequent pages, and
report the total in `meta.count`. `PaginatedCollectionBuilder` drafts the
requests for one such collection; the endpoint link and the parameters that
apply to every request against it are fixed at construction. Consumers
include deep search results, composite contents, and metadata revisions.
The page-following loops are I/O-driven and live in the client classes.

A container's children are the richest such collection: their search and
navigation requests carry encoded queries and (per-direction) sorting, so
`ChildrenSearchBuilder` specializes `PaginatedCollectionBuilder` with the
parameter encoding and the container-specific drafts (key lookup within
search results, `/distinct`).

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

    def _draft(self, url: str, params: dict[str, Any]) -> RequestDraft:
        return RequestDraft(
            "GET",
            url,
            params={**self.fixed_params, **params},
            headers={"Accept": MSGPACK_MIME_TYPE},
        )

    def first_page_link(self, offset: int, limit: Optional[int] = None) -> str:
        """The link to the first page of the collection, starting at `offset`.

        Each page of results links to the next, so only the first page's link
        is constructed client-side.
        """
        link = f"{self.link}?page[offset]={offset}"
        if limit is not None:
            link += f"&page[limit]={limit}"
        return link

    def draft_request_read_page(
        self, page_link: str, extra_params: Optional[dict[str, Any]] = None
    ) -> RequestDraft:
        """Draft the request fetching one page of the collection.

        `page_link` is the first page's link (see `first_page_link`) or a
        server-provided `links["next"]`; its baked pagination query is
        preserved by the draft's query folding. The fixed parameters are
        re-applied to every page.
        """
        return self._draft(page_link, extra_params or {})

    def draft_request_count(self) -> RequestDraft:
        "Draft the request counting the collection's entries."
        return self._draft(self.link, {"fields": "count"})

    @staticmethod
    def parse_count(content: JSON) -> int:
        "Extract the entry count from a collection response."
        return content["meta"]["count"]


@dataclass(frozen=True)
class ChildrenSearchBuilder(PaginatedCollectionBuilder):
    """The request builders for one container's (filtered) children, via `/search`.

    A container client's queries and sorting are fixed for its lifetime, so
    their encoded query parameters are computed once, at construction, and the
    search/navigation requests are drafted from them. The inherited `link` is
    the container's search link, and the inherited `fixed_params` hold the
    queries encoded as `filter[...]` parameters -- they apply to every
    request. Sorting participates per draft (reversed for backwards
    iteration), so it is held separately.

    Attributes
    ----------
    sorting_params : dict
        The sorting encoded as the `sort` query parameter.
    reversed_sorting_params : dict
        The reverse of the sorting, for iterating entries backwards.
    """

    sorting_params: dict[str, Any] = field(default_factory=dict)
    reversed_sorting_params: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_queries(
        cls,
        search_link: str,
        queries: Sequence[Any],
        sorting: Sequence[tuple[str, int]],
    ) -> "ChildrenSearchBuilder":
        "Encode the queries and sorting once, for all drafts built from them."
        return cls(
            link=search_link,
            fixed_params=queries_to_params(*queries),
            sorting_params=cls._sorting_params(sorting),
            reversed_sorting_params=cls._reversed_sorting_params(sorting),
        )

    @staticmethod
    def _encoded_sorting(sorting: Sequence[tuple[str, int]]) -> list[str]:
        # In the Python API sorting is encoded as (key, direction) tuples; in the
        # HTTP API each becomes "key" (ascending) or "-key" (descending).
        return [f"{'-' if direction < 0 else ''}{key}" for key, direction in sorting]

    @staticmethod
    def _sorting_params(sorting: Sequence[tuple[str, int]]) -> dict[str, list[str]]:
        "Encode a [(key, direction), ...] sorting as the `sort` query parameter."
        sorting_list = ChildrenSearchBuilder._encoded_sorting(sorting)
        return {"sort": sorting_list} if "".join(sorting_list) else {}

    @staticmethod
    def _reversed_sorting_params(
        sorting: Sequence[tuple[str, int]],
    ) -> dict[str, list[str]]:
        "Encode the reverse of `sorting`, for iterating a node's entries backwards."
        reversed_sorting_list = [
            item[1:] if item.startswith("-") else f"-{item.lstrip('+')}"
            for item in ChildrenSearchBuilder._encoded_sorting(sorting)
        ]
        return {"sort": reversed_sorting_list} if "".join(reversed_sorting_list) else {}

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

    def _directed_sorting_params(self, direction: int) -> dict[str, Any]:
        return self.sorting_params if direction > 0 else self.reversed_sorting_params

    def draft_request_count(self) -> RequestDraft:
        "Draft the request counting the container's (filtered) entries."
        return self._draft(self.link, {"fields": "count", **self.sorting_params})

    def draft_request_read_page(
        self,
        page_link: str,
        *,
        keys_only: bool,
        include_data_sources: bool = False,
        direction: int = 1,
    ) -> RequestDraft:
        """Draft the request fetching one page of the container's entries.

        `page_link` is the first page's link (see `first_page_link`) or a
        server-provided `links["next"]`; its baked pagination query is
        preserved by the draft's query folding. With `keys_only`, no attribute
        fields are requested: only the entry ids are needed. A negative
        `direction` iterates the entries backwards (reversed sorting).
        """
        params = dict(self._directed_sorting_params(direction))
        if keys_only:
            params["fields"] = ""
        if include_data_sources:
            params["include_data_sources"] = True
        return self._draft(page_link, params)

    def draft_request_lookup_key(
        self, key: str, *, include_data_sources: bool = False
    ) -> RequestDraft:
        """Draft the request looking up `key` within the search results.

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
        return self._draft(self.link, params)

    def draft_request_read_distinct(
        self,
        self_link: str,
        metadata_keys: Sequence[str],
        structure_families: bool,
        specs: bool,
        counts: bool,
    ) -> RequestDraft:
        "Draft the request fetching distinct metadata values, via `/distinct`."
        return self._draft(
            distinct_link(self_link),
            {
                "metadata": metadata_keys,
                "structure_families": structure_families,
                "specs": specs,
                "counts": counts,
            },
        )
