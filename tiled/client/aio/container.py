"""EXPERIMENTAL: the asynchronous container client.

`AsyncContainer` mirrors `tiled.client.container.Container` over the same
protocol layer. It is not a `Mapping` -- dunder lookups cannot await -- so
the interface is explicit coroutines and async iterators:

>>> node = await container.get("x")        # instead of container["x"]
>>> async for key in container: ...        # keys, like the sync client
>>> async for key, node in container.items(): ...
>>> await container.len()                  # instead of len(container)
"""

from __future__ import annotations

import asyncio
from typing import Any, Iterable, Optional, Union

import httpx
import stamina

from ...structures.core import StructureFamily
from .._protocol.node import draft_request_delete_node, draft_request_lookup_node
from ..container import CommonContainer, CommonDeepSearchResults
from ..utils import (
    TILED_RETRY_ATTEMPTS,
    TILED_RETRY_TIMEOUT,
    ClientError,
    client_for_item,
    collapse_key_segments,
    should_retry,
)
from .base import AsyncBaseClient

JSON = dict[str, Any]


class AsyncContainer(CommonContainer, AsyncBaseClient):
    def __repr__(self) -> str:
        path = "/".join(self.item["attributes"]["ancestors"] + [self.item["id"] or ""])
        return f"<{type(self).__name__} {path}>"

    async def len(self) -> int:
        "The number of entries, honoring any active queries."
        if (length := self._length_from_cache()) is not None:
            return length
        builder = self._children_search_builder
        content = (await self.context.send(builder.draft_request_count())).json()
        return self._cache_length(builder.parse_response_count(content))

    async def __aiter__(self):
        async for key in self.keys():
            yield key

    async def _iter_search_pages(
        self, keys_only: bool, page_size: Optional[int] = None
    ):
        "Iterate over the pages of entries that match the active queries."
        builder = self._children_search_builder
        next_page_url = builder.first_page_link(0, page_size)
        while next_page_url is not None:
            draft = builder.draft_request_read_page(
                next_page_url,
                keys_only=keys_only,
                include_data_sources=not keys_only and self._include_data_sources,
            )
            content = (await self.context.send(draft)).json()
            self._cache_length(builder.parse_response_count(content))
            yield content["data"]
            next_page_url = content["links"]["next"]

    async def keys(self, page_size: Optional[int] = None):
        "Iterate over the entry keys, in pages."
        async for page in self._iter_search_pages(keys_only=True, page_size=page_size):
            for item in page:
                yield item["id"]

    async def items(self, page_size: Optional[int] = None):
        "Iterate over (key, client) pairs, in pages."
        async for page in self._iter_search_pages(keys_only=False, page_size=page_size):
            for item in page:
                yield item["id"], client_for_item(
                    self.context,
                    self.structure_clients,
                    item,
                    include_data_sources=self._include_data_sources,
                )

    async def values(self, page_size: Optional[int] = None):
        "Iterate over the entry clients, in pages."
        async for _key, value in self.items(page_size=page_size):
            yield value

    async def get(self, *keys: str, default: Any = None):
        """Fetch the client for one (possibly nested) entry, or `default`.

        Each key may itself contain slashes: `await c.get("a/b", "c")` is
        equivalent to `await c.get("a", "b", "c")`.
        """
        try:
            return await self.lookup(*keys)
        except KeyError:
            return default

    async def lookup(self, *keys: str):
        """Fetch the client for one (possibly nested) entry.

        Raises KeyError if no such entry exists. The async counterpart of
        the synchronous `Container.__getitem__`.
        """
        key_segments = collapse_key_segments(keys)
        if self._queries:
            # A lookup under queries is itself a search; resolve one segment
            # at a time so that each level's filters apply.
            builder = self._children_search_builder
            draft = builder.draft_request_lookup_key(
                key_segments[0], include_data_sources=self._include_data_sources
            )
            content = (await self.context.send(draft)).json()
            data = content["data"]
            if not data:
                raise KeyError(key_segments[0])
            assert len(data) == 1
            result = client_for_item(
                self.context,
                self.structure_clients,
                data[0],
                include_data_sources=self._include_data_sources,
            )
            if key_segments[1:]:
                return await result.lookup(*key_segments[1:])
            return result
        draft = draft_request_lookup_node(
            self.item["links"]["self"],
            key_segments,
            include_data_sources=self._include_data_sources,
        )
        try:
            content = (await self.context.send(draft)).json()
        except ClientError as err:
            if err.response.status_code == httpx.codes.NOT_FOUND:
                err_arg = key_segments if len(key_segments) > 1 else key_segments[0]
                raise KeyError(err_arg) from None
            raise
        return client_for_item(
            self.context,
            self.structure_clients,
            content["data"],
            include_data_sources=self._include_data_sources,
        )

    def search_deep(self, *queries, max_depth=None):
        """
        Search all descendants of this container (at any depth), not just
        direct children.

        Returns an `AsyncDeepSearchResults`; see the synchronous
        `Container.search_deep` for semantics and examples.
        """
        if not queries:
            raise TypeError("search_deep() requires at least one query")
        if "search_deep" not in self.item["links"]:
            raise NotImplementedError("This server does not support deep search.")
        return AsyncDeepSearchResults(
            context=self.context,
            link=self.item["links"]["search_deep"],
            structure_clients=self.structure_clients,
            queries=queries,
            max_depth=max_depth,
            include_data_sources=self._include_data_sources,
            # The server reports `ancestors` relative to the true root, like
            # every other endpoint; strip this container's own path so that
            # result keys stay relative to the container that `search_deep`
            # was called on.
            root_path_parts=[part for part in self.path_parts if part],
        )

    async def distinct(
        self, *metadata_keys, structure_families=False, specs=False, counts=False
    ):
        """
        Get the unique values and optionally counts of metadata_keys,
        structure_families, and specs in this container's entries.

        See the synchronous `Container.distinct` for examples.
        """
        draft = self._children_search_builder.draft_request_read_distinct(
            self.item["links"]["self"],
            metadata_keys,
            structure_families,
            specs,
            counts,
        )
        return (await self.context.send(draft)).json()

    async def delete_contents(
        self,
        keys: Optional[Union[str, Iterable[str]]] = None,
        recursive: bool = False,
        external_only: bool = True,
    ) -> "AsyncContainer":
        """Delete the contents of this container.

        See the synchronous `Container.delete_contents` for parameters.
        """
        self._cached_len = None
        if keys is None:
            keys = [key async for key in self.keys()]
        keys = [keys] if isinstance(keys, str) else keys
        for key in set(keys):
            draft = draft_request_delete_node(
                self.uri, key, recursive=recursive, external_only=external_only
            )
            await self.context.send(draft)
        return self

    async def new(
        self,
        structure_family,
        data_sources,
        *,
        key=None,
        metadata=None,
        specs=None,
        access_tags=None,
    ):
        """Create a new node within this container.

        This is a low-level method. See high-level convenience methods listed below.

        See Also
        --------
        write_array
        create_container
        """
        self._cached_len = None
        create_recipe = self._create_node_recipe(
            structure_family,
            data_sources,
            key=key,
            metadata=metadata,
            specs=specs,
            access_tags=access_tags,
        )

        # All failure semantics live in the recipe's predicates; this loop is
        # purely mechanical: send, consult, act, re-raise. Its one piece of
        # state, `may_have_reached_server`, only ever turns True. The draft is
        # single-attempt (`RetryPolicy.NONE`): the create flow owns its
        # retries, tracking state across attempts.
        may_have_reached_server = False
        async for attempt in stamina.retry_context(
            on=should_retry, attempts=TILED_RETRY_ATTEMPTS, timeout=TILED_RETRY_TIMEOUT
        ):
            with attempt:
                try:
                    content = (await self.context.send(create_recipe.draft)).json()
                except Exception as exc:
                    if create_recipe.should_try_adopt(exc, may_have_reached_server):
                        existing = await self._adopt_after_retried_create_conflict(
                            create_recipe
                        )
                        if existing is not None:
                            return existing
                    may_have_reached_server = (
                        may_have_reached_server
                        or create_recipe.request_may_have_reached_server(exc)
                    )
                    raise

        item = create_recipe.merge(content)
        return client_for_item(
            self.context,
            self.structure_clients,
            item,
            structure=item["attributes"]["structure"],
            include_data_sources=self._include_data_sources,
        )

    async def _adopt_after_retried_create_conflict(self, create_recipe):
        """Fetch and return an existing node after a retried create hit 409 Conflict.

        The async counterpart of the synchronous
        `Container._adopt_after_retried_create_conflict`; see its docstring
        for the adoption conditions and trade-offs.
        """
        try:
            existing = await self.lookup(create_recipe.key)
        except Exception:
            existing = None
        return self._adopt_if_matching(create_recipe, existing)

    async def create_container(
        self, key=None, *, metadata=None, specs=None, access_tags=None
    ):
        """Create a new, empty container.

        See the synchronous `Container.create_container` for details.
        """
        return await self.new(
            StructureFamily.container,
            [],
            key=key,
            metadata=metadata,
            specs=specs,
            access_tags=access_tags,
        )

    async def write_array(
        self,
        array,
        *,
        key=None,
        metadata=None,
        dims=None,
        specs=None,
        access_tags=None,
    ):
        """
        EXPERIMENTAL: Write an array.

        See the synchronous `Container.write_array` for details. An array too
        large for one request is written in chunks, concurrently (bounded by
        the context's connection limit).
        """
        import numpy

        from ...ndslice import NDBlock
        from .._protocol.array import data_source_from_array

        if not (hasattr(array, "shape") and hasattr(array, "dtype")):
            # This does not implement enough of the array-like interface.
            # Coerce to numpy.
            array = numpy.asarray(array)

        data_source = data_source_from_array(
            array, dims, self._SUGGESTED_MAX_UPLOAD_SIZE
        )
        client = await self.new(
            StructureFamily.array,
            [data_source],
            key=key,
            metadata=metadata,
            specs=specs,
            access_tags=access_tags,
        )
        chunks = data_source.structure.chunks
        chunked = any(len(dim) > 1 for dim in chunks)
        if not chunked:
            await client.write(array)
        else:
            # Fan out one write per chunk.
            await asyncio.gather(
                *(
                    client._write_block(
                        array[NDBlock(*block).slice_from_chunks(chunks)], block
                    )
                    for block in NDBlock(...).chunk_indices(chunks)
                )
            )
        return client

    async def write_awkward(
        self, array, *, key=None, metadata=None, dims=None, specs=None, access_tags=None
    ):
        """
        Write an AwkwardArray.

        See the synchronous `Container.write_awkward` for parameters.
        """
        import awkward

        from ...structures.awkward import AwkwardStructure
        from ...structures.data_source import DataSource

        packed = awkward.to_packed(array)
        form, length, container = awkward.to_buffers(packed)
        structure = AwkwardStructure(length=length, form=form.to_dict())
        client = await self.new(
            StructureFamily.awkward,
            [DataSource(structure=structure, structure_family=StructureFamily.awkward)],
            key=key,
            metadata=metadata,
            specs=specs,
            access_tags=access_tags,
        )
        await client.write(container)
        return client

    async def write_sparse(
        self,
        coords,
        data,
        shape,
        *,
        key=None,
        metadata=None,
        dims=None,
        specs=None,
        access_tags=None,
    ):
        """
        EXPERIMENTAL: Write a sparse array (a single-chunk COO array).

        See the synchronous `Container.write_sparse` for parameters and
        examples.
        """
        # Deferred import: `_protocol.sparse` imports numpy and sparse at module scope.
        from .._protocol.sparse import data_source_from_coo

        client = await self.new(
            StructureFamily.sparse,
            [data_source_from_coo(coords, data, shape, dims)],
            key=key,
            metadata=metadata,
            specs=specs,
            access_tags=access_tags,
        )
        await client.write(coords, data)
        return client

    async def write_ragged(
        self,
        array,
        *,
        key=None,
        metadata=None,
        dims=None,
        specs=None,
        access_tags=None,
        max_partition_bytes=None,
        appendable=True,
    ):
        """Write a ragged array.

        See the synchronous `Container.write_ragged` for parameters. An array
        too large for one request is written in blocks -- sequentially, as
        ragged storage is designed for a single producer per dataset.
        """
        if not appendable:
            raise NotImplementedError("appendable=False is not yet implemented")

        import numpy

        from ...structures.ragged import make_ragged_array
        from .._protocol.ragged import data_source_from_ragged

        array = make_ragged_array(array)
        data_source = data_source_from_ragged(
            array, dims, max_partition_bytes or self._SUGGESTED_MAX_UPLOAD_SIZE
        )
        client = await self.new(
            StructureFamily.ragged,
            [data_source],
            key=key,
            metadata=metadata,
            specs=specs,
            access_tags=access_tags,
        )
        if not client.chunked:
            await client.write(array)
        else:
            divisions = numpy.cumsum((0, *data_source.structure.chunks[0]))
            starts, stops = divisions[:-1], divisions[1:]
            # The blocks are written sequentially, NOT concurrently: ragged
            # storage is designed for a single producer per dataset, and its
            # SQL backend does not accept concurrent chunk writes.
            for block_id, (start, stop) in enumerate(zip(starts, stops, strict=True)):
                await client._write_block(array[start:stop], block_id)
        return client

    async def write_table(
        self, data, *, key=None, metadata=None, specs=None, access_tags=None
    ):
        """Write tabular data (a pandas DataFrame or dict of arrays).

        See the synchronous `Container.write_table` for parameters. Unlike
        the synchronous client, dask dataframes are not supported.
        """
        from .._protocol.table import data_source_from_table

        client = await self.new(
            StructureFamily.table,
            [data_source_from_table(data)],
            key=key,
            metadata=metadata or {},
            specs=specs or [],
            access_tags=access_tags,
        )
        await client.write(data)
        return client

    async def create_appendable_table(
        self,
        schema,
        npartitions: int = 1,
        *,
        key=None,
        metadata=None,
        specs=None,
        access_tags=None,
        table_name: Optional[str] = None,
    ):
        """Initialize a table whose rows can be appended to a partition.

        See the synchronous `Container.create_appendable_table` for
        parameters.
        """
        from .._protocol.table import data_source_for_appendable_table

        return await self.new(
            StructureFamily.table,
            [data_source_for_appendable_table(schema, npartitions, table_name)],
            key=key,
            metadata=metadata or {},
            specs=specs or [],
            access_tags=access_tags,
        )


class AsyncDeepSearchResults(CommonDeepSearchResults):
    """Lazy, paginated results of `AsyncContainer.search_deep(...)`.

    Keys are tuples of path segments relative to the container that the
    search was performed on. Values are ordinary live async client nodes.
    Unlike the synchronous `DeepSearchResults`, this is not a `Mapping`
    (dunder protocols cannot await): use `await results.len()`,
    `async for` over `keys()` / `values()` / `items()`, and
    `await results.lookup(key)` / `get(key)`.
    """

    def __repr__(self) -> str:
        return f"<{type(self).__name__}>"

    async def len(self) -> int:
        "The number of results."
        if (length := self._length_from_cache()) is not None:
            return length
        draft = self._pages_builder.draft_request_count()
        content = (await self.context.send(draft)).json()
        return self._cache_length(self._pages_builder.parse_response_count(content))

    async def items(self, page_size: Optional[int] = None):
        "Iterate over (key, client) pairs, in pages."
        next_page_url = self._pages_builder.first_page_link(0, page_size)
        while next_page_url is not None:
            draft = self._pages_builder.draft_request_read_page(next_page_url)
            content = (await self.context.send(draft)).json()
            self._cache_length(self._pages_builder.parse_response_count(content))
            for item in content["data"]:
                yield self._result_key(item), client_for_item(
                    self.context,
                    self.structure_clients,
                    item,
                    include_data_sources=self._include_data_sources,
                )
            next_page_url = content["links"]["next"]

    async def keys(self, page_size: Optional[int] = None):
        "Iterate over the result keys, in pages."
        async for key, _value in self.items(page_size=page_size):
            yield key

    async def values(self, page_size: Optional[int] = None):
        "Iterate over the result clients, in pages."
        async for _key, value in self.items(page_size=page_size):
            yield value

    async def __aiter__(self):
        async for key in self.keys():
            yield key

    async def lookup(self, key):
        """Fetch the client for the result at `key`, a path or path segments.

        Raises KeyError if the key is not among the results. The async
        counterpart of the synchronous `DeepSearchResults.__getitem__`.
        """
        if isinstance(key, str):
            key = tuple(key.strip("/").split("/"))
        async for existing_key, value in self.items():
            if existing_key == key:
                return value
        raise KeyError(key)

    async def get(self, key, default=None):
        "Fetch the client for the result at `key`, or `default`."
        try:
            return await self.lookup(key)
        except KeyError:
            return default
