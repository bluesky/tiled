import collections
import collections.abc
from dataclasses import fields
import importlib
import itertools
import time
import warnings

import entrypoints

from ..query_registration import query_registry
from ..queries import KeyLookup
from ..utils import (
    OneShotCachedMap,
    Sentinel,
)
from .base import BaseClient
from ..trees.utils import (
    tree_repr,
    IndexersMixin,
    UNCHANGED,
)


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
                "structured_array_generic": lambda: importlib.import_module(
                    "..array", Node.__module__
                ).ArrayClient,
                "structured_array_tabular": lambda: importlib.import_module(
                    "..array", Node.__module__
                ).ArrayClient,
                "dataframe": lambda: importlib.import_module(
                    "..dataframe", Node.__module__
                ).DataFrameClient,
                "variable": lambda: importlib.import_module(
                    "..xarray", Node.__module__
                ).VariableClient,
                "data_array": lambda: importlib.import_module(
                    "..xarray", Node.__module__
                ).DataArrayClient,
                "dataset": lambda: importlib.import_module(
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
                "structured_array_generic": lambda: importlib.import_module(
                    "..array", Node.__module__
                ).DaskArrayClient,
                "structured_array_tabular": lambda: importlib.import_module(
                    "..array", Node.__module__
                ).DaskArrayClient,
                "dataframe": lambda: importlib.import_module(
                    "..dataframe", Node.__module__
                ).DaskDataFrameClient,
                "variable": lambda: importlib.import_module(
                    "..xarray", Node.__module__
                ).DaskVariableClient,
                "data_array": lambda: importlib.import_module(
                    "..xarray", Node.__module__
                ).DaskDataArrayClient,
                "dataset": lambda: importlib.import_module(
                    "..xarray", Node.__module__
                ).DaskDatasetClient,
            }
        ),
    }

    # This is populated when the first instance is created. To populate or
    # refresh it manually, call classmethod discover_special_clients().
    DEFAULT_SPECIAL_CLIENT_DISPATCH = None

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
    def discover_special_clients(cls):
        """
        Search the software environment for libraries that register special clients.

        This is called once automatically the first time Node.from_uri
        is called. You may call it again manually to refresh, and it will
        reflect any changes to the environment since it was first populated.
        """
        # The modules associated with these entrypoints will be imported
        # lazily, only when the item is first accessed.
        cls.DEFAULT_SPECIAL_CLIENT_DISPATCH = cls._discover_entrypoints(
            "tiled.special_client"
        )
        # Note: We could use entrypoints to discover custom structure_family types as
        # well, and in fact we did do this in an early draft. It was removed
        # for simplicity, at least for now.

    def __init__(
        self,
        context,
        *,
        path,
        item,
        structure_clients,
        special_clients,
        params=None,
        queries=None,
        sorting=None,
    ):
        "This is not user-facing. Use Node.from_uri."

        self.structure_clients = structure_clients
        self.special_clients = special_clients
        self._queries = list(queries or [])
        self._queries_as_params = _queries_to_params(*self._queries)
        sorting = item["attributes"].get("sorting")
        self._sorting = [(name, int(direction)) for name, direction in (sorting or [])]
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

    def touch(self):
        """
        Access all the data in this Node.

        This causes it to be cached if the context is configured with a cache.
        """
        self.context.get_json(self.uri)
        repr(self)
        for key in self:
            entry = self[key]
            entry.touch()

    def _get_class(self, item):
        # The server can use specs to tell us that this is not just *any*
        # node/array/dataframe/etc. but that is matches a certain specification
        # for which there may be a special client available.
        # Check each spec in order for a matching special client. Use the first
        # one we find. If we find no special client for any spec, fall back on
        # the defaults.
        specs = item["attributes"].get("specs", []) or []
        for spec in specs:
            class_ = self.special_clients.get(spec)
            if class_ is None:
                continue
            return class_
        if item["type"] == "reader":
            structure_family = item["attributes"]["structure_family"]
            try:
                return self.structure_clients[structure_family]
            except KeyError:
                raise UnknownStructureFamily(structure_family) from None
        return self.structure_clients["node"]

    def client_for_item(self, item, path):
        """
        Create an instance of the appropriate client class for an item.

        This is intended primarily for internal use and use by subclasses.
        """
        class_ = self._get_class(item)
        if item["type"] == "tree":
            return class_(
                context=self.context,
                item=item,
                path=path,
                structure_clients=self.structure_clients,
                special_clients=self.special_clients,
                params=self._params,
                queries=None,  # This is the only difference.
            )
        elif item["type"] == "reader":
            return class_(
                context=self.context,
                item=item,
                path=path,
                params=self._params,
            )
        else:
            raise NotImplementedError(
                f"Server sent item of unrecognized type {item['type']}"
            )

    def new_variation(
        self,
        *,
        structure_clients=UNCHANGED,
        special_clients=UNCHANGED,
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
        if special_clients is UNCHANGED:
            special_clients = self.special_clients
        if queries is UNCHANGED:
            queries = self._queries
        if sorting is UNCHANGED:
            sorting = self._sorting
        return super().new_variation(
            context=self.context,
            structure_clients=structure_clients,
            special_clients=special_clients,
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
        assert stop >= 0
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
        assert stop >= 0
        next_page_url = f"{self.item['links']['search']}?page[offset]={start}"
        item_counter = itertools.count(start)
        while next_page_url is not None:
            content = self.context.get_json(
                next_page_url,
                params={
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
            params={
                **self._queries_as_params,
                **sorting_params,
                **self._params,
            },
        )
        self._cached_len = (
            content["meta"]["count"],
            time.monotonic() + LENGTH_CACHE_TTL,
        )
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
        return self.new_variation(
            queries=self._queries + [query],
        )

    def sort(self, sorting):
        """
        Make a Node with the same entries but sorted according to `sorting`.

        Examples
        --------

        Sort by "color" in ascending order, and then by "height" in descending order.

        >>> from tiled.client import ASCENDING, DESCENDING
        >>> tree.sort([("color", ASCENDING), ("height", DESCENDING)])

        Note that ``1`` may be used as a synonym for ``ASCENDING``, and ``-1``
        may be used as a synonym for ``DESCENDING``.
        """
        return self.new_variation(
            sorting=sorting,
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
