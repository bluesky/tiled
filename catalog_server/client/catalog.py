import collections
import collections.abc
from dataclasses import fields
import itertools
import warnings

import entrypoints
import httpx

from ..query_registration import query_type_to_name
from ..queries import KeyLookup
from ..utils import catalog_repr, DictView, LazyMap, IndexCallable, slice_to_interval


class ClientCatalog(collections.abc.Mapping):

    # These are populated when the first instance is created. To populate or
    # refresh them manually, call classmethods discover_containers() and
    # discover_special_clients() respectively.
    DEFAULT_CONTAINER_DISPATCH = None
    DEFAULT_SPECIAL_CLIENT_DISPATCH = None

    @classmethod
    def _discover_entrypoints(cls, entrypoint_name):
        return LazyMap(
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

        This is called once automatically the first time ClientCatalog is
        instantiated. You may call it again manually to refresh, and it will
        reflect any changes to the environment since it was first populated.
        """
        # The modules associated with these entrypoints will be imported
        # lazily, only when the item is first accessed.
        cls.DEFAULT_SPECIAL_CLIENT_DISPATCH = cls._discover_entrypoints(
            "catalog_server.special_client"
        )

    @classmethod
    def discover_containers(cls):
        """
        Search the software environment for libraries that register containers.

        This is called once automatically the first time ClientCatalog is
        instantiated. You may call it again manually to refresh, and it will
        reflect any changes to the environment since it was first populated.
        """
        cls.DEFAULT_CONTAINER_DISPATCH = cls._discover_entrypoints(
            "catalog_server.container_client"
        )

    def __init__(
        self,
        client,
        *,
        path,
        metadata,
        container_dispatch=None,
        special_client_dispatch=None,
        params=None,
        queries=None,
    ):
        "This is not user-facing. Use ClientCatalog.from_uri."

        # We do entrypoint discovery the first time this is instantiated rather
        # than at import time, in order to make import faster.
        if self.DEFAULT_SPECIAL_CLIENT_DISPATCH is None:
            self.discover_special_clients()
        if self.DEFAULT_CONTAINER_DISPATCH is None:
            self.discover_containers()

        self._client = client
        self._metadata = metadata
        self.container_dispatch = collections.ChainMap(
            container_dispatch or {},
            self.DEFAULT_CONTAINER_DISPATCH,
        )
        self.special_client_dispatch = collections.ChainMap(
            special_client_dispatch or {},
            self.DEFAULT_SPECIAL_CLIENT_DISPATCH,
        )
        if isinstance(path, str):
            raise ValueError("path is expected to be a list of segments")
        # Stash *immutable* copies just to be safe.
        self._path = tuple(path or [])
        self._queries = tuple(queries or [])
        self._queries_as_params = _queries_to_params(*self._queries)
        self._params = params or {}
        self.keys_indexer = IndexCallable(self._keys_indexer)
        self.items_indexer = IndexCallable(self._items_indexer)
        self.values_indexer = IndexCallable(self._values_indexer)

    @classmethod
    def from_uri(
        cls, uri, token, container_dispatch=None, special_client_dispatch=None
    ):
        client = httpx.Client(
            base_url=uri.rstrip("/"),
            headers={"X-Access-Token": token},
        )
        response = client.get("/metadata/")
        response.raise_for_status()
        metadata = response.json()["data"]["attributes"]["metadata"]
        return cls(
            client,
            path=[],
            metadata=metadata,
            container_dispatch=container_dispatch,
            special_client_dispatch=special_client_dispatch,
        )

    def __repr__(self):
        # Display the first N keys to avoid making a giant service request.
        # Use _keys_slicer because it is unauthenticated.
        N = 5
        return catalog_repr(self, self._keys_slice(0, N))

    @property
    def metadata(self):
        "Metadata about this Catalog."
        # Ensure this is immutable (at the top level) to help the user avoid
        # getting the wrong impression that editing this would update anything
        # persistent.
        return DictView(self._metadata)

    def _get_class(self, item):
        "Return the appropriate Client object for this structure."
        # The basic structure of the response is either one of the containers
        # we know about or a sub-Catalog. The server can hint that we should
        # use a special variant that might have a special __repr__, or extra
        # methods for usability, etc.
        client_type_hint = item["attributes"].get("client_type_hint")
        if client_type_hint is not None:
            try:
                cls = self.special_client_dispatch[client_type_hint]
            except KeyError:
                warnings.warn(
                    "The server suggested to use a special client with the "
                    f"hint {client_type_hint!r} but nothing matching the "
                    "description could be discovered in the current software "
                    "environment. We will fall back back to a default that "
                    "should be functional but may lack some usability "
                    "features."
                )
            else:
                return cls
        if item["type"] == "catalog":
            cls = type(self)
        else:
            cls = self.container_dispatch[item["attributes"]["container"]]
        return cls

    def __len__(self):
        response = self._client.get(
            f"/search/{'/'.join(self._path)}",
            params={"fields": "", **self._queries_as_params, **self._params},
        )
        response.raise_for_status()
        return response.json()["meta"]["count"]

    def __length_hint__(self):
        # TODO The server should provide an estimated count.
        # https://www.python.org/dev/peps/pep-0424/
        return len(self)

    def __iter__(self):
        next_page_url = f"/search/{'/'.join(self._path)}"
        while next_page_url is not None:
            response = self._client.get(
                next_page_url,
                params={"fields": "", **self._queries_as_params, **self._params},
            )
            response.raise_for_status()
            for item in response.json()["data"]:
                yield item["id"]
            next_page_url = response.json()["links"]["next"]

    def __getitem__(self, key):
        # Lookup this key *within the search results* of this Catalog.
        response = self._client.get(
            f"/search/{'/'.join(self._path )}",
            params={
                "fields": ["metadata", "container", "client_type_hint"],
                **_queries_to_params(KeyLookup(key)),
                **self._queries_as_params,
                **self._params,
            },
        )
        response.raise_for_status()
        data = response.json()["data"]
        if not data:
            raise KeyError(key)
        assert (
            len(data) == 1
        ), "The key lookup query must never result more than one result."
        (item,) = data
        cls = self._get_class(item)
        return cls(
            client=self._client,
            path=self._path + (item["id"],),
            metadata=item["attributes"]["metadata"],
            container_dispatch=self.container_dispatch,
            special_client_dispatch=self.special_client_dispatch,
            params=self._params,
        )

    def items(self):
        # The base implementation would use __iter__ and __getitem__, making
        # one HTTP request per item. Pull pages instead.
        next_page_url = f"/search/{'/'.join(self._path)}"
        while next_page_url is not None:
            response = self._client.get(
                next_page_url,
                params={
                    "fields": ["metadata", "container", "client_type_hint"],
                    **self._queries_as_params,
                    **self._params,
                },
            )
            response.raise_for_status()
            for item in response.json()["data"]:
                key = item["id"]
                cls = self._get_class(item)
                value = cls(
                    self._client,
                    path=self._path + (item["id"],),
                    metadata=item["attributes"]["metadata"],
                    container_dispatch=self.container_dispatch,
                    special_client_dispatch=self.special_client_dispatch,
                    params=self._params,
                )
                yield key, value
            next_page_url = response.json()["links"]["next"]

    def values(self):
        # The base implementation would use __iter__ and __getitem__, making
        # one HTTP request per item. Pull pages instead.
        for _, value in self.items():
            yield value

    def _keys_slice(self, start, stop):
        next_page_url = f"/search/{'/'.join(self._path)}?page[offset]={start}"
        item_counter = itertools.count(start)
        while next_page_url is not None:
            response = self._client.get(
                next_page_url,
                params={"fields": "", **self._queries_as_params, **self._params},
            )
            response.raise_for_status()
            for item in response.json()["data"]:
                if stop is not None and next(item_counter) == stop:
                    return
                yield item["id"]
            next_page_url = response.json()["links"]["next"]

    def _items_slice(self, start, stop):
        next_page_url = f"/search/{'/'.join(self._path)}?page[offset]={start}"
        item_counter = itertools.count(start)
        while next_page_url is not None:
            response = self._client.get(
                next_page_url,
                params={
                    "fields": ["metadata", "container", "client_type_hint"],
                    **self._queries_as_params,
                    **self._params,
                },
            )
            response.raise_for_status()
            for item in response.json()["data"]:
                if stop is not None and next(item_counter) == stop:
                    return
                key = item["id"]
                cls = self._get_class(item)
                yield key, cls(
                    self._client,
                    path=self._path + (item["id"],),
                    metadata=item["attributes"]["metadata"],
                    container_dispatch=self.container_dispatch,
                    special_client_dispatch=self.special_client_dispatch,
                    params=self._params,
                )
            next_page_url = response.json()["links"]["next"]

    def _item_by_index(self, index):
        if index >= len(self):
            raise IndexError(f"index {index} out of range for length {len(self)}")
        url = f"/search/{'/'.join(self._path)}?page[offset]={index}&page[limit]=1"
        response = self._client.get(
            url,
            params={
                "fields": ["metadata", "container", "client_type_hint"],
                **self._queries_as_params,
                **self._params,
            },
        )
        response.raise_for_status()
        (item,) = response.json()["data"]
        key = item["id"]
        cls = self._get_class(item)
        value = cls(
            self._client,
            path=self._path + (item["id"],),
            metadata=item["attributes"]["metadata"],
            container_dispatch=self.container_dispatch,
            special_client_dispatch=self.special_client_dispatch,
            params=self._params,
        )
        return (key, value)

    def search(self, query):
        return type(self)(
            client=self._client,
            path=self._path,
            queries=self._queries + (query,),
            metadata=self._metadata,
            container_dispatch=self.container_dispatch,
            special_client_dispatch=self.special_client_dispatch,
        )

    def _keys_indexer(self, index):
        if isinstance(index, int):
            key, _value = self._item_by_index(index)
            return key
        elif isinstance(index, slice):
            start, stop = slice_to_interval(index)
            return list(self._keys_slice(start, stop))
        else:
            raise TypeError(f"{index} must be an int or slice, not {type(index)}")

    def _items_indexer(self, index):
        if isinstance(index, int):
            return self._item_by_index(index)
        elif isinstance(index, slice):
            start, stop = slice_to_interval(index)
            return list(self._items_slice(start, stop))
        else:
            raise TypeError(f"{index} must be an int or slice, not {type(index)}")

    def _values_indexer(self, index):
        if isinstance(index, int):
            _key, value = self._item_by_index(index)
            return value
        elif isinstance(index, slice):
            start, stop = slice_to_interval(index)
            return [value for _key, value in self._items_slice(start, stop)]
        else:
            raise TypeError(f"{index} must be an int or slice, not {type(index)}")


def _queries_to_params(*queries):
    "Compute GET params from the queries."
    params = {}
    for query in queries:
        name = query_type_to_name[type(query)]
        for field in fields(query):
            params[f"filter[{name}][condition][{field.name}]"] = getattr(
                query, field.name
            )
    return params
