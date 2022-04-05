import collections
import collections.abc
import importlib
import itertools
import time
import warnings
from dataclasses import fields

import entrypoints

from ..adapters.utils import IndexersMixin, tree_repr
from ..queries import KeyLookup
from ..query_registration import query_registry
from ..utils import UNCHANGED, OneShotCachedMap, Sentinel
from .base import BaseClient
from .cache import Revalidate, verify_cache
from .utils import export_util


class Node(BaseClient, collections.abc.Mapping, IndexersMixin):

    # This maps the structure_family sent by the server to a client-side object that
    # can interpret the structure_family's structure and content. OneShotCachedMap is used to
    # defer imports.
    DEFAULT_STRUCTURE_CLIENT_DISPATCH = {
        "numpy": OneShotCachedMap(
            {
                "node": lambda: Node,
                "array": lambda: importlib.import_module(
                    "..array", Node.__module__
                ).ArrayClient,
                "dataframe": lambda: importlib.import_module(
                    "..dataframe", Node.__module__
                ).DataFrameClient,
                "variable": lambda: importlib.import_module(
                    "..xarray", Node.__module__
                ).VariableClient,
                "xarray_data_array": lambda: importlib.import_module(
                    "..xarray", Node.__module__
                ).DataArrayClient,
                "xarray_dataset": lambda: importlib.import_module(
                    "..xarray", Node.__module__
                ).DatasetClient,
            }
        ),
        "dask": OneShotCachedMap(
            {
                "node": lambda: Node,
                "array": lambda: importlib.import_module(
                    "..array", Node.__module__
                ).DaskArrayClient,
                "dataframe": lambda: importlib.import_module(
                    "..dataframe", Node.__module__
                ).DaskDataFrameClient,
                "variable": lambda: importlib.import_module(
                    "..xarray", Node.__module__
                ).DaskVariableClient,
                "xarray_data_array": lambda: importlib.import_module(
                    "..xarray", Node.__module__
                ).DaskDataArrayClient,
                "xarray_dataset": lambda: importlib.import_module(
                    "..xarray", Node.__module__
                ).DaskDatasetClient,
            }
        ),
    }

    # This is populated when the first instance is created.
    STRUCTURE_CLIENTS_FROM_ENTRYPOINTS = None

    @classmethod
    def _discover_entrypoints(cls, entrypoint_name):
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
                cls.DEFAULT_STRUCTURE_CLIENT_DISPATCH["numpy"].set(
                    name, entrypoint.load
                )
                cls.DEFAULT_STRUCTURE_CLIENT_DISPATCH["dask"].set(name, entrypoint.load)

    def __init__(
        self,
        context,
        *,
        path,
        item,
        structure_clients,
        params=None,
        queries=None,
        sorting=None,
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
            path=path,
            params=params,
            structure_clients=structure_clients,
        )

    def __repr__(self):
        # Display up to the first N keys to avoid making a giant service
        # request. Use _keys_slicer because it is unauthenticated.
        N = 10
        return tree_repr(self, self._keys_slice(0, N, direction=1))

    @property
    def sorting(self):
        """
        The current sorting of this Node

        Given as a list of tuples where the first entry is the sorting key
        and the second entry indicates ASCENDING (or 1) or DESCENDING (or -1).
        """
        return list(self._sorting)

    def download(self):
        """
        Access all the data in this Node.

        This causes it to be cached if the context is configured with a cache.
        """
        verify_cache(self.context.cache)
        self.context.get_json(self.uri)
        repr(self)
        for key in self:
            entry = self[key]
            entry.download()

    def refresh(self, force=False):
        """
        Refresh cached data for this node.

        Parameters
        ----------
        force: bool
            If False, (default) refresh only expired cache entries.
            If True, refresh all cache entries.
        """
        if force:
            revalidate = Revalidate.FORCE
        else:
            revalidate = Revalidate.IF_EXPIRED
        with self.context.revalidation(revalidate):
            self.download()

    def client_for_item(self, item, path):
        """
        Create an instance of the appropriate client class for an item.

        This is intended primarily for internal use and use by subclasses.
        """
        # The server can use specs to tell us that this is not just *any*
        # node/array/dataframe/etc. but that is matches a certain specification
        # for which there may be a special client available.
        # Check each spec in order for a matching structure client. Use the first
        # one we find. If we find no structure client for any spec, fall back on
        # the default for this structure family.
        specs = item["attributes"].get("specs", []) or []
        for spec in specs:
            class_ = self.structure_clients.get(spec)
            if class_ is not None:
                break
        else:
            structure_family = item["attributes"]["structure_family"]
            try:
                class_ = self.structure_clients[structure_family]
            except KeyError:
                raise UnknownStructureFamily(structure_family) from None
        return class_(
            context=self.context,
            item=item,
            path=path,
            structure_clients=self.structure_clients,
            params=self._params,
        )

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
            structure_clients = Node.DEFAULT_STRUCTURE_CLIENT_DISPATCH[
                structure_clients
            ]
        if structure_clients is UNCHANGED:
            structure_clients = self.structure_clients
        if queries is UNCHANGED:
            queries = self._queries
        if sorting is UNCHANGED:
            sorting = self._sorting
        return super().new_variation(
            context=self.context,
            structure_clients=structure_clients,
            queries=queries,
            sorting=sorting,
            **kwargs,
        )

    def __len__(self):
        now = time.monotonic()
        if self._cached_len is not None:
            length, deadline = self._cached_len
            if now < deadline:
                # Used the cached value and do not make any request.
                return length
        content = self.context.get_json(
            self.item["links"]["search"],
            params={
                "fields": "",
                **self._queries_as_params,
                **self._sorting_params,
                **self._params,
            },
        )
        length = content["meta"]["count"]
        self._cached_len = (length, now + LENGTH_CACHE_TTL)
        return length

    def __length_hint__(self):
        # TODO The server should provide an estimated count.
        # https://www.python.org/dev/peps/pep-0424/
        return len(self)

    def __iter__(self):
        next_page_url = self.item["links"]["search"]
        while next_page_url is not None:
            content = self.context.get_json(
                next_page_url,
                params={
                    "fields": "",
                    **self._queries_as_params,
                    **self._sorting_params,
                    **self._params,
                },
            )
            self._cached_len = (
                content["meta"]["count"],
                time.monotonic() + LENGTH_CACHE_TTL,
            )
            for item in content["data"]:
                yield item["id"]
            next_page_url = content["links"]["next"]

    def __getitem__(self, key):
        # These are equivalent:
        #
        # >>> node['a']['b']['c']
        # >>> node[('a', 'b', 'c')]
        # >>> node['a', 'b', 'c']
        #
        # The last two are equivalent at a Python level;
        # both call node.__getitem__(('a', 'b', 'c')).
        #
        # TODO Elide this into a single request to the server rather than
        # a chain of requests. This is not totally straightforward because
        # of this use case:
        #
        # >>> node.search(...)['a', 'b']
        #
        # which must only return a result if 'a' is contained in the search results.
        # There are also some open design questions on the server side about
        # how search and tree-traversal will relate, so we'll wait to make this
        # optimization until that is fully worked out.
        if isinstance(key, tuple):
            child = self
            for k in key:
                child = child[k]
            return child

        # Lookup this key *within the search results* of this Node.
        content = self.context.get_json(
            self.item["links"]["search"],
            params={
                **_queries_to_params(KeyLookup(key)),
                **self._queries_as_params,
                **self._sorting_params,
                **self._params,
            },
        )
        self._cached_len = (
            content["meta"]["count"],
            time.monotonic() + LENGTH_CACHE_TTL,
        )
        data = content["data"]
        if not data:
            raise KeyError(key)
        assert (
            len(data) == 1
        ), "The key lookup query must never result more than one result."
        (item,) = data
        return self.client_for_item(item, path=self._path + (item["id"],))

    def items(self):
        # The base implementation would use __iter__ and __getitem__, making
        # one HTTP request per item. Pull pages instead.
        next_page_url = self.item["links"]["search"]
        while next_page_url is not None:
            content = self.context.get_json(
                next_page_url,
                params={
                    **self._queries_as_params,
                    **self._sorting_params,
                    **self._params,
                },
            )
            self._cached_len = (
                content["meta"]["count"],
                time.monotonic() + LENGTH_CACHE_TTL,
            )
            for item in content["data"]:
                key = item["id"]
                value = self.client_for_item(item, path=self._path + (item["id"],))
                yield key, value
            next_page_url = content["links"]["next"]

    def values(self):
        # The base implementation would use __iter__ and __getitem__, making
        # one HTTP request per item. Pull pages instead.
        for _, value in self.items():
            yield value

    # The following three methods are used by IndexersMixin
    # to define keys_indexer, items_indexer, and values_indexer.

    def _keys_slice(self, start, stop, direction):
        if direction > 0:
            sorting_params = self._sorting_params
        else:
            sorting_params = self._reversed_sorting_params
        assert start >= 0
        assert (stop is None) or (stop >= 0)
        next_page_url = f"{self.item['links']['search']}?page[offset]={start}"
        item_counter = itertools.count(start)
        while next_page_url is not None:
            content = self.context.get_json(
                next_page_url,
                params={
                    "fields": "",
                    **self._queries_as_params,
                    **sorting_params,
                    **self._params,
                },
            )
            self._cached_len = (
                content["meta"]["count"],
                time.monotonic() + LENGTH_CACHE_TTL,
            )
            for item in content["data"]:
                if stop is not None and next(item_counter) == stop:
                    return
                yield item["id"]
            next_page_url = content["links"]["next"]

    def _items_slice(self, start, stop, direction):
        if direction > 0:
            sorting_params = self._sorting_params
        else:
            sorting_params = self._reversed_sorting_params
        assert start >= 0
        assert (stop is None) or (stop >= 0)
        next_page_url = f"{self.item['links']['search']}?page[offset]={start}"
        item_counter = itertools.count(start)
        while next_page_url is not None:
            content = self.context.get_json(
                next_page_url,
                params={**self._queries_as_params, **sorting_params, **self._params},
            )
            self._cached_len = (
                content["meta"]["count"],
                time.monotonic() + LENGTH_CACHE_TTL,
            )

            for item in content["data"]:
                if stop is not None and next(item_counter) == stop:
                    return
                key = item["id"]
                yield key, self.client_for_item(item, path=self._path + (item["id"],))
            next_page_url = content["links"]["next"]

    def _item_by_index(self, index, direction):
        if direction > 0:
            sorting_params = self._sorting_params
        else:
            sorting_params = self._reversed_sorting_params
        assert index >= 0
        next_page_url = (
            f"{self.item['links']['search']}?page[offset]={index}&page[limit]=1"
        )
        content = self.context.get_json(
            next_page_url,
            params={**self._queries_as_params, **sorting_params, **self._params},
        )
        self._cached_len = (
            content["meta"]["count"],
            time.monotonic() + LENGTH_CACHE_TTL,
        )
        if not content["data"]:
            raise IndexError("Index out of range.")
        (item,) = content["data"]
        key = item["id"]
        value = self.client_for_item(item, path=self._path + (item["id"],))
        return (key, value)

    def search(self, query):
        """
        Make a Node with a subset of this Node's entries, filtered by query.

        Examples
        --------

        >>> from tiled.queries import FullText
        >>> tree.search(FullText("hello"))
        """
        return self.new_variation(queries=self._queries + [query])

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

    def export(self, filepath, format=None):
        """
        Download all metadata and data below this node in some format and write to a file.

        Parameters
        ----------
        file: str or buffer
            Filepath or writeable buffer.
        format : str, optional
            If format is None and `file` is a filepath, the format is inferred
            from the name, like 'table.h5' implies format="application/x-hdf5". The format
            may be given as a file extension ("h5") or a media type ("application/x-hdf5").

        Examples
        --------

        Export all.

        >>> a.export("everything.h5")

        """
        return export_util(
            filepath,
            format,
            self.context.get_content,
            self.item["links"]["full"],
            params={},
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
                    "Tab-completition is not supported on this particular Node "
                    "because it has a large number of entries."
                )
                warnings.warn(MSG)
                return []
            else:
                return list(self)
        except Exception:
            # Do not print messy traceback from thread. Just fail silently.
            return []


def _queries_to_params(*queries):
    "Compute GET params from the queries."
    params = collections.defaultdict(list)
    for query in queries:
        name = query_registry.query_type_to_name[type(query)]
        for field in fields(query):
            value = getattr(query, field.name)
            if isinstance(value, (list, tuple)):
                for item_as_str in map(str, value):
                    if "," in item_as_str:
                        raise ValueError(
                            "Items in list- or tuple-type parameters may not contain commas."
                        )
                value = ",".join(map(str, value))
            if value is not None:
                params[f"filter[{name}][condition][{field.name}]"].append(value)
    return dict(params)


class UnknownStructureFamily(KeyError):
    pass


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
"Decending sort order. An alias for -1."
