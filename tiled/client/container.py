import collections
import collections.abc
import functools
import importlib
import itertools
import time
import warnings
from dataclasses import asdict
from typing import TYPE_CHECKING, Any, Iterable, Optional, Union
from urllib.parse import parse_qs, urlparse

import entrypoints
import httpx
import orjson

from ..adapters.utils import IndexersMixin
from ..iterviews import ItemsView, KeysView, ValuesView
from ..queries import KeyLookup
from ..query_registration import default_query_registry
from ..structures.core import StructureFamily
from ..structures.data_source import DataSource
from ..utils import UNCHANGED, OneShotCachedMap, Sentinel, node_repr, safe_json_dump
from .base import STRUCTURE_TYPES, BaseClient
from .utils import (
    MSGPACK_MIME_TYPE,
    ClientError,
    client_for_item,
    export_util,
    handle_error,
    normalize_specs,
    retry_context,
)

if TYPE_CHECKING:
    import pandas
    import pyarrow


class Container(BaseClient, collections.abc.Mapping, IndexersMixin):
    # This maps the structure_family sent by the server to a client-side object that
    # can interpret the structure_family's structure and content. OneShotCachedMap is used to
    # defer imports.

    # This is populated when the first instance is created.
    STRUCTURE_CLIENTS_FROM_ENTRYPOINTS = None

    @classmethod
    def _discover_entrypoints(cls, entrypoint_name) -> OneShotCachedMap[str, Any]:
        return OneShotCachedMap(
            {
                name: entrypoint.load
                for name, entrypoint in entrypoints.get_group_named(
                    entrypoint_name
                ).items()
            }
        )

    @classmethod
    def discover_clients_from_entrypoints(cls):
        """
        Search the software environment for libraries that register structure clients.

        This is called once automatically the first time Node.from_uri
        is called. It is idempotent.
        """
        if cls.STRUCTURE_CLIENTS_FROM_ENTRYPOINTS is not None:
            # short-circuit
            return
        # The modules associated with these entrypoints will be imported
        # lazily, only when the item is first accessed.
        cls.STRUCTURE_CLIENTS_FROM_ENTRYPOINTS = OneShotCachedMap()
        # Check old name (special_client) and new name (structure_client).
        for entrypoint_name in ["tiled.special_client", "tiled.structure_client"]:
            for name, entrypoint in entrypoints.get_group_named(
                entrypoint_name
            ).items():
                cls.STRUCTURE_CLIENTS_FROM_ENTRYPOINTS.set(name, entrypoint.load)
                DEFAULT_STRUCTURE_CLIENT_DISPATCH["numpy"].set(name, entrypoint.load)
                DEFAULT_STRUCTURE_CLIENT_DISPATCH["dask"].set(name, entrypoint.load)

    def __init__(
        self,
        context,
        *,
        item,
        structure_clients,
        queries=None,
        sorting=None,
        structure=None,
        include_data_sources=False,
    ):
        "This is not user-facing. Use Node.from_uri."

        self.structure_clients = structure_clients
        self._queries = list(queries or [])
        self._queries_as_params = _queries_to_params(*self._queries)
        # If the user has not specified a sorting, give the server the opportunity
        # to tell us the default sorting.
        if sorting:
            self._sorting = sorting
        else:
            # In the Python API we encode sorting as (key, direction).
            # This order-based "record" notion does not play well with OpenAPI.
            # In the HTTP API, therefore, we use {"key": key, "direction": direction}.
            self._sorting = [
                (s["key"], int(s["direction"]))
                for s in (item["attributes"].get("sorting") or [])
            ]
        sorting = sorting or item["attributes"].get("sorting")
        self._sorting_params = {
            "sort": ",".join(
                f"{'-' if item[1] < 0 else ''}{item[0]}" for item in self._sorting
            )
        }
        self._reversed_sorting_params = {
            "sort": ",".join(
                f"{'-' if item[1] > 0 else ''}{item[0]}" for item in self._sorting
            )
        }
        super().__init__(
            context=context,
            item=item,
            structure_clients=structure_clients,
            include_data_sources=include_data_sources,
        )

    def __repr__(self):
        # Display up to the first N keys to avoid making a giant service
        # request. Use _keys_slicer because it is unauthenticated.
        N = 10
        return node_repr(self, self._keys_slice(0, N, direction=1))

    @property
    def sorting(self):
        """
        The current sorting of this Node

        Given as a list of tuples where the first entry is the sorting key
        and the second entry indicates ASCENDING (or 1) or DESCENDING (or -1).
        """
        return list(self._sorting)

    def new_variation(
        self,
        *,
        structure_clients=UNCHANGED,
        queries=UNCHANGED,
        sorting=UNCHANGED,
        **kwargs,
    ):
        """
        Create a copy of this Node, optionally varying some parameters.

        This is intended primarily for internal use and use by subclasses.
        """
        if isinstance(structure_clients, str):
            structure_clients = DEFAULT_STRUCTURE_CLIENT_DISPATCH[structure_clients]
        if structure_clients is UNCHANGED:
            structure_clients = self.structure_clients
        if queries is UNCHANGED:
            queries = self._queries
        if sorting is UNCHANGED:
            sorting = self._sorting
        return super().new_variation(
            structure_clients=structure_clients,
            queries=queries,
            sorting=sorting,
            **kwargs,
        )

    def __len__(self):
        # If the contents of this node was provided in-line, there is an
        # implication that the contents are not expected to be dynamic. Used the
        # count provided in the structure.
        if contents := self.item["attributes"]["structure"]["contents"]:
            return len(contents)

        now = time.monotonic()
        if self._cached_len is not None:
            length, deadline = self._cached_len
            if now < deadline:
                # Used the cached value and do not make any request.
                return length
        link = self.item["links"]["search"]
        for attempt in retry_context():
            with attempt:
                content = handle_error(
                    self.context.http_client.get(
                        link,
                        headers={"Accept": MSGPACK_MIME_TYPE},
                        params={
                            **parse_qs(urlparse(link).query),
                            "fields": "count",
                            **self._queries_as_params,
                            **self._sorting_params,
                        },
                    )
                ).json()
        length = content["meta"]["count"]
        self._cached_len = (length, now + LENGTH_CACHE_TTL)
        return length

    def __length_hint__(self):
        # TODO The server should provide an estimated count.
        # https://www.python.org/dev/peps/pep-0424/
        return len(self)

    def __iter__(self, _ignore_inlined_contents=False):
        # If the contents of this node was provided in-line, and we don't need
        # to apply any filtering or sorting, we can slice the in-lined data
        # without fetching anything from the server.
        contents = self.item["attributes"]["structure"]["contents"]
        if (
            (contents is not None)
            and (not self._queries)
            and ((not self.sorting) or (self.sorting == [("_", 1)]))
            and (not _ignore_inlined_contents)
        ):
            return (yield from contents)
        next_page_url = self.item["links"]["search"]
        while next_page_url is not None:
            for attempt in retry_context():
                with attempt:
                    content = handle_error(
                        self.context.http_client.get(
                            next_page_url,
                            headers={"Accept": MSGPACK_MIME_TYPE},
                            params={
                                **parse_qs(urlparse(next_page_url).query),
                                "fields": "",
                                **self._queries_as_params,
                                **self._sorting_params,
                            },
                        )
                    ).json()
            self._cached_len = (
                content["meta"]["count"],
                time.monotonic() + LENGTH_CACHE_TTL,
            )
            for item in content["data"]:
                yield item["id"]
            next_page_url = content["links"]["next"]

    def __getitem__(self, keys, _ignore_inlined_contents=False):
        # These are equivalent:
        #
        # >>> node['a']['b']['c']
        # >>> node[('a', 'b', 'c')]
        # >>> node['a', 'b', 'c']
        #
        # The last two are equivalent at a Python level;
        # both call node.__getitem__(('a', 'b', 'c')).
        #
        # We elide this into a single request to the server rather than
        # a chain of requests. This is not totally straightforward because
        # of this use case:
        #
        # >>> node.search(...)['a', 'b']
        #
        # which must only return a result if 'a' is contained in the search results.
        if not isinstance(keys, tuple):
            keys = (keys,)
        for key in keys:
            if not isinstance(key, str):
                raise TypeError("Containers can only be indexed by strings")
        keys = tuple("/".join(keys).strip("/").split("/"))  # Remove any slashes
        if self._queries:
            # Lookup this key *within the search results* of this Node.
            key, *tail = keys
            tail = tuple(tail)  # list -> tuple
            params = {
                **_queries_to_params(KeyLookup(key)),
                **self._queries_as_params,
                **self._sorting_params,
            }
            if self._include_data_sources:
                params["include_data_sources"] = True
            link = self.item["links"]["search"]
            for attempt in retry_context():
                with attempt:
                    content = handle_error(
                        self.context.http_client.get(
                            link,
                            headers={"Accept": MSGPACK_MIME_TYPE},
                            params={**parse_qs(urlparse(link).query), **params},
                        )
                    ).json()
            self._cached_len = (
                content["meta"]["count"],
                time.monotonic() + LENGTH_CACHE_TTL,
            )
            data = content["data"]
            if not data:
                raise KeyError(key)
            assert (
                len(data) == 1
            ), "The key lookup query must never return more than one result."
            (item,) = data
            result = client_for_item(
                self.context,
                self.structure_clients,
                item,
                include_data_sources=self._include_data_sources,
            )
            if tail:
                result = result[tail]
        else:
            # Straightforwardly look up the keys under this node.
            # There is no search filter in place, so if it is there
            # then we want it.

            # The server may greedily send nested information about children
            # ("inlined contents") to reduce latency. This is how we handle
            # xarray Datasets efficiently, for example.

            # In a loop, walk the key(s). Use inlined contents if we have it.
            # When we reach a key that we don't have inlined contents for, send
            # out a single request with all the rest of the keys, and break
            # the keys-walking loop. We are effectively "jumping" down the tree
            # to the node of interest without downloading information about
            # intermediate parents.
            for i, key in enumerate(keys):
                item = (self.item["attributes"]["structure"]["contents"] or {}).get(key)
                if item is not None and not _ignore_inlined_contents:
                    # The item was inlined, and we should return it as is
                    result = client_for_item(
                        self.context,
                        self.structure_clients,
                        item,
                        include_data_sources=self._include_data_sources,
                    )
                    return (
                        result[keys[i + 1 :]] if keys[i + 1 :] else result  # noqa: E203
                    )
                else:
                    # The item was not inlined, either because nothing was inlined
                    # or because it was added after we fetched the inlined contents.
                    # Make a request for it.
                    try:
                        self_link = self.item["links"]["self"].rstrip("/")
                        params = {}
                        if self._include_data_sources:
                            params["include_data_sources"] = True
                        link = self_link + "".join(f"/{key}" for key in keys[i:])
                        for attempt in retry_context():
                            with attempt:
                                content = handle_error(
                                    self.context.http_client.get(
                                        link,
                                        headers={"Accept": MSGPACK_MIME_TYPE},
                                        params={
                                            **parse_qs(urlparse(link).query),
                                            **params,
                                        },
                                    )
                                ).json()
                    except ClientError as err:
                        if err.response.status_code == httpx.codes.NOT_FOUND:
                            # If this is a scalar lookup, raise KeyError("X") not KeyError(("X",)).
                            err_arg = keys[i:]
                            if len(err_arg) == 1:
                                (err_arg,) = err_arg
                            raise KeyError(err_arg)
                        raise
                    item = content["data"]
                    break

            result = client_for_item(
                self.context,
                self.structure_clients,
                item,
                include_data_sources=self._include_data_sources,
            )
        return result

    def delete_contents(
        self,
        keys: Optional[Union[str, Iterable[str]]] = None,
        recursive: bool = False,
        external_only: bool = True,
    ) -> "Container":
        """Delete the contents of this Container.

        Parameters
        ----------
        keys : str or list of str
            The key(s) to delete. If a list, all keys in the list will be deleted.
            If None (default), delete all contents.
        recursive : bool, optional
            If True, descend into sub-containers and delete their contents too.
            Defaults to False.
        external_only : bool, optional
            If True, only delete externally-managed data. Defaults to True.
        """

        self._cached_len = None
        if keys is None:
            keys = self.keys()
        keys = [keys] if isinstance(keys, str) else keys
        for key in set(keys):
            for attempt in retry_context():
                with attempt:
                    handle_error(
                        self.context.http_client.delete(
                            f"{self.uri}/{key}",
                            params={
                                "recursive": recursive,
                                "external_only": external_only,
                            },
                        )
                    )

        return self

    # The following two methods are used by keys(), values(), items().

    def _keys_slice(
        self, start, stop, direction, page_size=None, *, _ignore_inlined_contents=False
    ):
        # If the contents of this node was provided in-line, and we don't need
        # to apply any filtering or sorting, we can slice the in-lined data
        # without fetching anything from the server.
        contents = self.item["attributes"]["structure"]["contents"]
        if (
            (contents is not None)
            and (not self._queries)
            and ((not self.sorting) or (self.sorting == [("_", 1)]))
            and (not _ignore_inlined_contents)
        ):
            keys = list(contents)
            if direction < 0:
                keys = list(reversed(keys))
            return (yield from keys[start:stop])
        if direction > 0:
            sorting_params = self._sorting_params
        else:
            sorting_params = self._reversed_sorting_params
        assert start >= 0
        assert (stop is None) or (stop >= 0)
        next_page_url = f"{self.item['links']['search']}?page[offset]={start}"
        if page_size is not None:
            next_page_url += f"&page[limit]={page_size}"
        item_counter = itertools.count(start)
        while next_page_url is not None:
            for attempt in retry_context():
                with attempt:
                    content = handle_error(
                        self.context.http_client.get(
                            next_page_url,
                            headers={"Accept": MSGPACK_MIME_TYPE},
                            params={
                                **parse_qs(urlparse(next_page_url).query),
                                "fields": "",
                                **self._queries_as_params,
                                **sorting_params,
                            },
                        )
                    ).json()
            self._cached_len = (
                content["meta"]["count"],
                time.monotonic() + LENGTH_CACHE_TTL,
            )
            for item in content["data"]:
                yield item["id"]
                if stop is not None and next(item_counter) == stop - 1:
                    return
            next_page_url = content["links"]["next"]

    def _items_slice(
        self, start, stop, direction, page_size=None, *, _ignore_inlined_contents=False
    ):
        # If the contents of this node was provided in-line, and we don't need
        # to apply any filtering or sorting, we can slice the in-lined data
        # without fetching anything from the server.
        contents = self.item["attributes"]["structure"]["contents"]
        if (
            (contents is not None)
            and (not self._queries)
            and ((not self.sorting) or (self.sorting == [("_", 1)]))
            and (not _ignore_inlined_contents)
        ):
            items = list(contents.items())
            if direction < 0:
                items = list(reversed(items))
            for key, item in items[start:stop]:
                yield key, client_for_item(
                    self.context,
                    self.structure_clients,
                    item,
                    include_data_sources=self._include_data_sources,
                )
            return
        if direction > 0:
            sorting_params = self._sorting_params
        else:
            sorting_params = self._reversed_sorting_params
        assert start >= 0
        assert (stop is None) or (stop >= 0)
        next_page_url = f"{self.item['links']['search']}?page[offset]={start}"
        if page_size is not None:
            next_page_url += f"&page[limit]={page_size}"
        item_counter = itertools.count(start)
        while next_page_url is not None:
            params = {
                **parse_qs(urlparse(next_page_url).query),
                **self._queries_as_params,
                **sorting_params,
            }
            if self._include_data_sources:
                params["include_data_sources"] = True
            for attempt in retry_context():
                with attempt:
                    content = handle_error(
                        self.context.http_client.get(
                            next_page_url,
                            headers={"Accept": MSGPACK_MIME_TYPE},
                            params=params,
                        )
                    ).json()
            self._cached_len = (
                content["meta"]["count"],
                time.monotonic() + LENGTH_CACHE_TTL,
            )
            for item in content["data"]:
                key = item["id"]
                yield key, client_for_item(
                    self.context,
                    self.structure_clients,
                    item,
                    include_data_sources=self._include_data_sources,
                )
                if stop is not None and next(item_counter) == stop - 1:
                    return
            next_page_url = content["links"]["next"]

    def keys(self):
        return KeysView(lambda: len(self), self._keys_slice)

    def values(self):
        return ValuesView(lambda: len(self), self._items_slice)

    def items(self):
        return ItemsView(lambda: len(self), self._items_slice)

    def search(self, query):
        """
        Make a Node with a subset of this Node's entries, filtered by query.

        Examples
        --------

        >>> from tiled.queries import FullText
        >>> tree.search(FullText("hello"))
        """
        return self.new_variation(queries=self._queries + [query])

    def distinct(
        self, *metadata_keys, structure_families=False, specs=False, counts=False
    ):
        """
        Get the unique values and optionally counts of metadata_keys,
        structure_families, and specs in this Node's entries

        Examples
        --------

        Query all the distinct values of a key.

        >>> tree.distinct("foo", counts=True)

        Query for multiple keys at once.

        >>> tree.distinct("foo", "bar", counts=True)
        """

        link = self.item["links"]["self"].replace("/metadata", "/distinct", 1)
        for attempt in retry_context():
            with attempt:
                distinct = handle_error(
                    self.context.http_client.get(
                        link,
                        headers={"Accept": MSGPACK_MIME_TYPE},
                        params={
                            **parse_qs(urlparse(link).query),
                            "metadata": metadata_keys,
                            "structure_families": structure_families,
                            "specs": specs,
                            "counts": counts,
                            **self._queries_as_params,
                        },
                    )
                ).json()
        return distinct

    def sort(self, *sorting):
        """
        Make a Node with the same entries but sorted according to `sorting`.

        Examples
        --------

        Sort by "color" in ascending order, and then by "height" in descending order.

        >>> from tiled.client import ASCENDING, DESCENDING
        >>> tree.sort(("color", ASCENDING), ("height", DESCENDING))

        Note that ``1`` may be used as a synonym for ``ASCENDING``, and ``-1``
        may be used as a synonym for ``DESCENDING``.
        """
        return self.new_variation(sorting=sorting)

    def export(self, filepath, fields=None, *, format=None):
        """
        Download metadata and data below this node in some format and write to a file.

        Parameters
        ----------
        file: str or buffer
            Filepath or writeable buffer.
        fields: List[str], optional
            Filter which items in this node to export.
        format : str, optional
            If format is None and `file` is a filepath, the format is inferred
            from the name, like 'table.h5' implies format="application/x-hdf5". The format
            may be given as a file extension ("h5") or a media type ("application/x-hdf5").

        Examples
        --------

        Export all.

        >>> a.export("everything.h5")

        """
        params = {}
        if fields is not None:
            params["field"] = fields
        return export_util(
            filepath,
            format,
            self.context.http_client.get,
            self.item["links"]["full"],
            params=params,
        )

    def _ipython_key_completions_(self):
        """
        Provide method for the key-autocompletions in IPython.

        See http://ipython.readthedocs.io/en/stable/config/integrating.html#tab-completion
        """
        MAX_ENTRIES_SUPPORTED = 40
        try:
            if len(self) > MAX_ENTRIES_SUPPORTED:
                MSG = (
                    "Tab-completion is not supported on this particular Node "
                    "because it has a large number of entries."
                )
                warnings.warn(MSG)
                return []
            else:
                return list(self)
        except Exception:
            # Do not print messy traceback from thread. Just fail silently.
            return []

    def new(
        self,
        structure_family,
        data_sources,
        *,
        key=None,
        metadata=None,
        specs=None,
        access_tags=None,
    ):
        """
        Create a new item within this Node.

        This is a low-level method. See high-level convenience methods listed below.

        See Also
        --------
        write_array
        write_dataframe
        write_coo_array
        """
        self._cached_len = None
        metadata = metadata or {}
        access_blob = {"tags": access_tags} if access_tags is not None else {}

        item = {
            "attributes": {
                "metadata": metadata,
                "structure_family": StructureFamily(structure_family),
                "specs": normalize_specs(specs or []),
                "data_sources": [asdict(data_source) for data_source in data_sources],
                "access_blob": access_blob,
            }
        }
        body = dict(item["attributes"])
        if key is not None:
            body["id"] = key

        # if check:
        if any(data_source.assets for data_source in data_sources):
            endpoint = self.uri.replace("/metadata/", "/register/", 1)
        else:
            endpoint = self.uri

        for attempt in retry_context():
            with attempt:
                document = handle_error(
                    self.context.http_client.post(
                        endpoint,
                        headers={"Accept": MSGPACK_MIME_TYPE},
                        content=safe_json_dump(body),
                    )
                ).json()

        if structure_family == StructureFamily.container:
            structure = {"contents": None, "count": None}
        else:
            # Only containers can have multiple data_sources right now.
            (data_source,) = data_sources
            structure = data_source.structure
        item["attributes"]["structure"] = structure

        # If server returned modified metadata update the local copy.
        # Otherwise, round-trip it through a JSON serializer locally
        # to ensure that any special types (e.g. datetimes, numpy arrays)
        # are normalized to natively JSON serializable types.
        if "metadata" in document:
            item["attributes"]["metadata"] = document.pop("metadata")
        else:
            item["attributes"]["metadata"] = orjson.loads(
                safe_json_dump(item["attributes"]["metadata"])
            )

        # Ditto for structure
        if "structure" in document:
            item["attributes"]["structure"] = STRUCTURE_TYPES[structure_family](
                document.pop("structure")
            )

        # And for data sources
        if "data_sources" in document:
            item["attributes"]["data_sources"] = [
                ds for ds in document.pop("data_sources")
            ]

        # And for access_blob
        if "access_blob" in document:
            item["attributes"]["access_blob"] = document.pop("access_blob")

        # Merge in "id" and "links" returned by the server.
        item.update(document)

        # Ensure this is a dataclass, not a dict.
        # When we apply type hints and mypy to the client it should be possible
        # to dispense with this.
        if (structure_family != StructureFamily.container) and isinstance(
            structure, dict
        ):
            structure_type = STRUCTURE_TYPES[structure_family]
            structure = structure_type.from_json(structure)

        return client_for_item(
            self.context,
            self.structure_clients,
            item,
            structure=structure,
            include_data_sources=self._include_data_sources,
        )

    # When (re)chunking arrays for upload, we use this limit
    # to attempt to avoid bumping into size limits.
    _SUGGESTED_MAX_UPLOAD_SIZE = 100_000_000  # 100 MB

    def create_container(
        self,
        key=None,
        *,
        metadata=None,
        specs=None,
        access_tags=None,
    ):
        """Create a new, empty container.

        Parameters
        ----------
        key : str, optional
            Key (name) for this new node. If None, the server will provide a unique key.
        metadata : dict, optional
            User metadata. May be nested. Must contain only basic types
            (e.g. numbers, strings, lists, dicts) that are JSON-serializable.
        specs : List[Spec], optional
            List of names that are used to label that the data and/or metadata
            conform to some named standard specification.
        access_tags: List[str], optional
            Server-specific authZ tags in list form, used to confer access to the node.

        """
        return self.new(
            StructureFamily.container,
            [],
            key=key,
            metadata=metadata,
            specs=specs,
            access_tags=access_tags,
        )

    def write_array(
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

        Parameters
        ----------
        array : array-like
        key : str, optional
            Key (name) for this new node. If None, the server will provide a unique key.
        metadata : dict, optional
            User metadata. May be nested. Must contain only basic types
            (e.g. numbers, strings, lists, dicts) that are JSON-serializable.
        dims : List[str], optional
            A label for each dimension of the array.
        specs : List[Spec], optional
            List of names that are used to label that the data and/or metadata
            conform to some named standard specification.
        access_tags: List[str], optional
            Server-specific authZ tags in list form, used to confer access to the node.

        """
        import dask.array
        import numpy
        from dask.array.core import normalize_chunks

        from ..structures.array import ArrayStructure, BuiltinDtype

        if not (hasattr(array, "shape") and hasattr(array, "dtype")):
            # This does not implement enough of the array-like interface.
            # Coerce to numpy.
            array = numpy.asarray(array)

        # Determine chunks such that each chunk is not too large to upload.
        # Any existing chunking will be taken into account.
        # If the array is small, there will be only one chunk.
        if hasattr(array, "chunks"):
            chunks = normalize_chunks(
                array.chunks,
                limit=self._SUGGESTED_MAX_UPLOAD_SIZE,
                dtype=array.dtype,
                shape=array.shape,
            )
        else:
            chunks = normalize_chunks(
                tuple("auto" for _ in array.shape),
                limit=self._SUGGESTED_MAX_UPLOAD_SIZE,
                dtype=array.dtype,
                shape=array.shape,
            )

        structure = ArrayStructure(
            shape=array.shape,
            chunks=chunks,
            dims=dims,
            data_type=BuiltinDtype.from_numpy_dtype(array.dtype),
        )
        client = self.new(
            StructureFamily.array,
            [
                DataSource(structure=structure, structure_family=StructureFamily.array),
            ],
            key=key,
            metadata=metadata,
            specs=specs,
            access_tags=access_tags,
        )
        chunked = any(len(dim) > 1 for dim in chunks)
        if not chunked:
            client.write(array)
        else:
            # Fan out client.write_block over each chunk using dask.
            if isinstance(array, dask.array.Array):
                da = array.rechunk(chunks)
            else:
                da = dask.array.from_array(array, chunks=chunks)

            # Dask inspects the signature and passes block_id in if present.
            # It also apparently calls it with an empty array and block_id
            # once, so we catch that call and become a no-op.
            def write_block(x, block_id, client):
                if len(block_id):
                    client.write_block(x, block=block_id)
                return x

            # TODO Is there a fire-and-forget analogue such that we don't need
            # to bother with the return type?
            da.map_blocks(write_block, dtype=da.dtype, client=client).compute()
        return client

    def write_awkward(
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
        Write an AwkwardArray.

        Parameters
        ----------
        array: awkward.Array
        key : str, optional
            Key (name) for this new node. If None, the server will provide a unique key.
        metadata : dict, optional
            User metadata. May be nested. Must contain only basic types
            (e.g. numbers, strings, lists, dicts) that are JSON-serializable.
        dims : List[str], optional
            A label for each dimension of the array.
        specs : List[Spec], optional
            List of names that are used to label that the data and/or metadata
            conform to some named standard specification.
        access_tags: List[str], optional
            Server-specific authZ tags in list form, used to confer access to the node.
        """
        import awkward

        from ..structures.awkward import AwkwardStructure

        packed = awkward.to_packed(array)
        form, length, container = awkward.to_buffers(packed)
        structure = AwkwardStructure(
            length=length,
            form=form.to_dict(),
        )
        client = self.new(
            StructureFamily.awkward,
            [
                DataSource(
                    structure=structure, structure_family=StructureFamily.awkward
                ),
            ],
            key=key,
            metadata=metadata,
            specs=specs,
            access_tags=access_tags,
        )
        client.write(container)
        return client

    def write_sparse(
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
        EXPERIMENTAL: Write a sparse array.

        Parameters
        ----------
        coords : array-like
        data : array-like
        shape : tuple
        key : str, optional
            Key (name) for this new node. If None, the server will provide a unique key.
        metadata : dict, optional
            User metadata. May be nested. Must contain only basic types
            (e.g. numbers, strings, lists, dicts) that are JSON-serializable.
        dims : List[str], optional
            A label for each dimension of the array.
        specs : List[Spec], optional
            List of names that are used to label that the data and/or metadata
            conform to some named standard specification.
        access_tags: List[str], optional
            Server-specific authZ tags in list form, used to confer access to the node.

        Examples
        --------

        Write a sparse.COO array.

        >>> import sparse
        >>> from numpy import array
        >>> coo = sparse.COO(coords=array([[2, 5]]), data=array([1.3, 7.5]), shape=(10,))
        >>> c.write_sparse(coords=coo.coords, data=coo.data, shape=coo.shape)

        This only supports a single chunk. For chunked upload, use lower-level methods.

        # Define the overall shape and the dimensions of each chunk.
        >>> from tiled.structures.sparse import COOStructure
        >>> structure = COOStructure(shape=(10,), chunks=((5, 5),))
        >>> data_source = DataSource(structure=structure, structure_family=StructureFamily.sparse)
        >>> x = c.new("sparse", [data_source])
        # Upload the data in each chunk.
        # Coords are given with in the reference frame of each chunk.
        >>> x.write_block(coords=array([[2, 4]]), data=array([3.1, 2.8]), block=(0,))
        >>> x.write_block(coords=array([[0, 1]]), data=array([6.7, 1.2]), block=(1,))
        """
        from ..structures.array import BuiltinDtype
        from ..structures.sparse import COOStructure

        structure = COOStructure(
            shape=shape,
            # This method only supports single-chunk COO arrays.
            chunks=tuple((dim,) for dim in shape),
            dims=dims,
            data_type=BuiltinDtype.from_numpy_dtype(data.dtype),
            coord_data_type=BuiltinDtype.from_numpy_dtype(coords.dtype),
        )
        client = self.new(
            StructureFamily.sparse,
            [
                DataSource(
                    structure=structure, structure_family=StructureFamily.sparse
                ),
            ],
            key=key,
            metadata=metadata,
            specs=specs,
            access_tags=access_tags,
        )
        client.write(coords, data)
        return client

    def create_appendable_table(
        self,
        schema: "pyarrow.Schema",
        npartitions: int = 1,
        *,
        key=None,
        metadata=None,
        specs=None,
        access_tags=None,
        table_name: Optional[str] = None,
    ):
        """Initialize a table whose rows can be appended to a partition.

        Parameters
        ----------
        schema : column names and dtypes info in the form of pyarrow.Schema
        npartitions : int, optional
            Number of partitions to create. Default is 1.
        key : str, optional
            Key (name) for this new node. If None, the server will provide a unique key.
        metadata : dict, optional
            User metadata. May be nested. Must contain only basic types
            (e.g. numbers, strings, lists, dicts) that are JSON-serializable.
        specs : List[Spec], optional
            List of names that are used to label that the data and/or metadata
            conform to some named standard specification.
        access_tags: List[str], optional
            Server-specific authZ tags in list form, used to confer access to the node.
        table_name : str, optional
            Optionally provide a name for the table this should be stored in.
            By default a name unique to the schema will be chosen.

        See Also
        --------
        write_dataframe
        """
        parameters = {}
        if table_name is not None:
            parameters["table_name"] = table_name

        from ..structures.table import TableStructure

        structure = TableStructure.from_schema(schema, npartitions)

        client = self.new(
            StructureFamily.table,
            [
                DataSource(
                    structure=structure,
                    structure_family=StructureFamily.table,
                    mimetype="application/x-tiled-sql-table",
                    parameters=parameters,
                )
            ],
            key=key,
            metadata=metadata or {},
            specs=specs or [],
            access_tags=access_tags,
        )

        return client

    def write_dataframe(
        self,
        dataframe: Union["pandas.DataFrame", dict[str, Any]],
        *,
        key=None,
        metadata=None,
        specs=None,
        access_tags=None,
    ):
        """Write a DataFrame.

        Parameters
        ----------
        dataframe : pandas.DataFrame or dict
        key : str, optional
            Key (name) for this new node. If None, the server will provide a unique key.
        metadata : dict, optional
            User metadata. May be nested. Must contain only basic types
            (e.g. numbers, strings, lists, dicts) that are JSON-serializable.
        specs : List[Spec], optional
            List of names that are used to label that the data and/or metadata
            conform to some named standard specification.
        access_tags: List[str], optional
            Server-specific authZ tags in list form, used to confer access to the node.

        See Also
        --------
        create_appendable_table
        """
        import dask.dataframe

        from ..structures.table import TableStructure

        if isinstance(dataframe, dask.dataframe.DataFrame):
            structure = TableStructure.from_dask_dataframe(dataframe)
        elif isinstance(dataframe, dict):
            # table as dict, e.g. {"a": [1,2,3], "b": [4,5,6]}
            structure = TableStructure.from_dict(dataframe)
        else:
            structure = TableStructure.from_pandas(dataframe)

        client = self.new(
            StructureFamily.table,
            [
                DataSource(
                    structure=structure,
                    structure_family=StructureFamily.table,
                )
            ],
            key=key,
            metadata=metadata or {},
            specs=specs or [],
            access_tags=access_tags,
        )

        if hasattr(dataframe, "partitions"):
            if isinstance(dataframe, dask.dataframe.DataFrame):
                ddf = dataframe
            else:
                raise NotImplementedError(
                    f"Unsure how to handle type {type(dataframe)}"
                )

            ddf.map_partitions(
                functools.partial(_write_partition, client=client), meta=dataframe._meta
            ).compute()
        else:
            client.write(dataframe)

        return client


def _queries_to_params(*queries):
    "Compute GET params from the queries."
    params = collections.defaultdict(list)
    for query in queries:
        name = default_query_registry.query_type_to_name[type(query)]
        for field, value in query.encode().items():
            if value is not None:
                params[f"filter[{name}][condition][{field}]"].append(value)
    return dict(params)


LENGTH_CACHE_TTL = 1  # second


class Ascending(Sentinel):
    "Intended for more readable sorting operations. An alias for 1."

    def __index__(self):
        return 1


class Descending(Sentinel):
    "Intended for more readable sorting operations. An alias for -1."

    def __index__(self):
        return -1


ASCENDING = Ascending("ASCENDING")
"Ascending sort order. An alias for 1."
DESCENDING = Descending("DESCENDING")
"Descending sort order. An alias for -1."


class _LazyLoad:
    # This exists because lambdas and closures cannot be pickled.
    def __init__(self, import_module_args, attr_name):
        self.import_module_args = import_module_args
        self.attr_name = attr_name

    def __call__(self):
        return getattr(
            importlib.import_module(*self.import_module_args), self.attr_name
        )


class _Wrap:
    # This exists because lambdas and closures cannot be pickled.
    def __init__(self, obj):
        self.obj = obj

    def __call__(self):
        return self.obj


def _write_partition(x, partition_info, client):
    client.write_partition(x, partition_info["number"])
    return x


DEFAULT_STRUCTURE_CLIENT_DISPATCH = {
    "numpy": OneShotCachedMap(
        {
            "container": _Wrap(Container),
            "composite": _LazyLoad(
                ("..composite", Container.__module__), "CompositeClient"
            ),
            "array": _LazyLoad(("..array", Container.__module__), "ArrayClient"),
            "awkward": _LazyLoad(("..awkward", Container.__module__), "AwkwardClient"),
            "dataframe": _LazyLoad(
                ("..dataframe", Container.__module__), "DataFrameClient"
            ),
            "sparse": _LazyLoad(("..sparse", Container.__module__), "SparseClient"),
            "table": _LazyLoad(
                ("..dataframe", Container.__module__), "DataFrameClient"
            ),
            "xarray_dataset": _LazyLoad(
                ("..xarray", Container.__module__), "DatasetClient"
            ),
        }
    ),
    "dask": OneShotCachedMap(
        {
            "container": _Wrap(Container),
            "composite": _LazyLoad(
                ("..composite", Container.__module__), "CompositeClient"
            ),
            "array": _LazyLoad(("..array", Container.__module__), "DaskArrayClient"),
            # TODO Create DaskAwkwardClient
            # "awkward": _LazyLoad(("..awkward", Container.__module__), "DaskAwkwardClient"),
            "dataframe": _LazyLoad(
                ("..dataframe", Container.__module__), "DaskDataFrameClient"
            ),
            "sparse": _LazyLoad(("..sparse", Container.__module__), "SparseClient"),
            "table": _LazyLoad(
                ("..dataframe", Container.__module__), "DaskDataFrameClient"
            ),
            "xarray_dataset": _LazyLoad(
                ("..xarray", Container.__module__), "DaskDatasetClient"
            ),
        }
    ),
}
