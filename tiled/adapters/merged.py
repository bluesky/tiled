"""
Adapter returned by `MapAdapter.search_recursive()` when the tree contains
mounted subtrees (e.g. a `CatalogNodeAdapter` mounted at a sub-path by
`tiled.config.Config.merged_trees`) that are not themselves `MapAdapter`s.

`MapAdapter.search_recursive()` can only flatten descendants reachable by
walking plain mappings. Any non-`MapAdapter` container child that supports
`search_recursive()` is instead placed in `mounts` and queried through its
own recursive-search interface; this adapter merges the flattened local
entries with the mounted subtrees' entries into a single logical sequence,
implementing the same duck-typed paging protocol as `CatalogNodeAdapter`
(see `tiled.server.core.construct_entries_response`).
"""

from typing import Any, Dict, List, Optional, Tuple

from ..structures.core import Spec, StructureFamily
from ..type_aliases import JSON
from ..utils import UNCHANGED


class MergedRecursiveAdapter:
    """Merge flattened map-native entries with one or more mounted subtrees."""

    structure_family = StructureFamily.container

    def __init__(
        self,
        flat: Dict[str, Any],
        mounts: List[Tuple[str, Any]],
        *,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
    ) -> None:
        self._flat = dict(flat)
        self._mounts = list(mounts)
        self._metadata = metadata or {}
        self._specs = specs or []

    def metadata(self) -> JSON:
        return self._metadata

    @property
    def specs(self) -> List[Spec]:
        return self._specs

    def new_variation(
        self,
        *,
        flat: Any = UNCHANGED,
        mounts: Any = UNCHANGED,
        metadata: Any = UNCHANGED,
        specs: Any = UNCHANGED,
    ) -> "MergedRecursiveAdapter":
        if flat is UNCHANGED:
            flat = self._flat
        if mounts is UNCHANGED:
            mounts = self._mounts
        if metadata is UNCHANGED:
            metadata = self._metadata
        if specs is UNCHANGED:
            specs = self._specs
        return type(self)(flat, mounts, metadata=metadata, specs=specs)

    def search(self, query: Any) -> "MergedRecursiveAdapter":
        # Deferred import to avoid a module-level circular import: mapping.py
        # imports this module (inside search_recursive) to build the result.
        from .mapping import MapAdapter

        local = MapAdapter(self._flat).search(query)
        new_mounts = [
            (prefix, subtree.search(query)) for prefix, subtree in self._mounts
        ]
        return self.new_variation(flat=dict(local.items()), mounts=new_mounts)

    async def _mount_len(self, subtree: Any) -> int:
        if hasattr(subtree, "exact_len"):
            return int(await subtree.exact_len())
        return len(subtree)

    async def _mount_keys(
        self, subtree: Any, offset: int, limit: Optional[int]
    ) -> List[str]:
        if hasattr(subtree, "keys_range"):
            return list(await subtree.keys_range(offset, limit))
        keys = list(subtree.keys())
        stop = None if limit is None else offset + limit
        return keys[offset:stop]

    async def _mount_items(
        self, subtree: Any, offset: int, limit: Optional[int]
    ) -> List[Tuple[str, Any]]:
        if hasattr(subtree, "items_range"):
            return list(await subtree.items_range(offset, limit))
        items = list(subtree.items())
        stop = None if limit is None else offset + limit
        return items[offset:stop]

    async def _paginate(
        self, offset: int, limit: Optional[int], *, keys_only: bool
    ) -> Tuple[List[Any], Optional[int]]:
        # The local (flat) part and each mount are concatenated, in order,
        # into one logical sequence; `remaining_offset`/`remaining_limit`
        # track our position as we walk across that concatenation.
        collected: List[Any] = []
        remaining_offset = offset
        remaining_limit = None if limit is None else limit + 1  # peek 1 for "has more"

        local_items = list(self._flat.items())
        if remaining_offset < len(local_items):
            stop = (
                None if remaining_limit is None else remaining_offset + remaining_limit
            )
            batch = local_items[remaining_offset:stop]
            collected.extend(key if keys_only else (key, value) for key, value in batch)
            remaining_offset = 0
            if remaining_limit is not None:
                remaining_limit -= len(batch)
        else:
            remaining_offset -= len(local_items)

        for prefix, subtree in self._mounts:
            if remaining_limit is not None and remaining_limit <= 0:
                break
            sub_len = await self._mount_len(subtree)
            if remaining_offset >= sub_len:
                remaining_offset -= sub_len
                continue
            if keys_only:
                sub_keys = await self._mount_keys(
                    subtree, remaining_offset, remaining_limit
                )
                collected.extend(f"{prefix}/{key}" for key in sub_keys)
                consumed = len(sub_keys)
            else:
                sub_items = await self._mount_items(
                    subtree, remaining_offset, remaining_limit
                )
                collected.extend((f"{prefix}/{key}", value) for key, value in sub_items)
                consumed = len(sub_items)
            remaining_offset = 0
            if remaining_limit is not None:
                remaining_limit -= consumed

        next_offset = None
        if limit is not None and len(collected) > limit:
            collected = collected[:limit]
            next_offset = offset + limit
        return collected, next_offset

    async def exact_len(self) -> int:
        total = len(self._flat)
        for _, subtree in self._mounts:
            total += await self._mount_len(subtree)
        return total

    async def cursor_for_offset(self, offset: int) -> Optional[int]:
        # Our cursor space is just the offset itself; see keys_page/items_page.
        return offset or None

    async def keys_page(
        self, cursor: Optional[int] = None, limit: Optional[int] = None
    ) -> Tuple[List[str], Optional[int]]:
        return await self._paginate(cursor or 0, limit, keys_only=True)

    async def items_page(
        self, cursor: Optional[int] = None, limit: Optional[int] = None
    ) -> Tuple[List[Tuple[str, Any]], Optional[int]]:
        return await self._paginate(cursor or 0, limit, keys_only=False)

    async def keys_range(
        self, offset: int = 0, limit: Optional[int] = None
    ) -> List[str]:
        keys, _ = await self._paginate(offset, limit, keys_only=True)
        return keys

    async def items_range(
        self, offset: int = 0, limit: Optional[int] = None
    ) -> List[Tuple[str, Any]]:
        items, _ = await self._paginate(offset, limit, keys_only=False)
        return items
