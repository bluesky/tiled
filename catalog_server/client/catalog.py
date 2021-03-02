import collections
import collections.abc
from dataclasses import fields
import importlib
import itertools

import httpx

from ..query_registration import query_type_to_name
from ..queries import KeyLookup
from ..utils import catalog_repr, DictView, LazyMap, IndexCallable, slice_to_interval


class ClientCatalog(collections.abc.Mapping):

    # This maps the container sent by the server to a client-side object that
    # can interpret the container's structure and content. LazyMap is used to
    # defer imports.
    DEFAULT_CONTAINER_DISPATCH = LazyMap(
        {
            "array": lambda: importlib.import_module(
                "..array", ClientCatalog.__module__
            ).ClientArraySource,
        }
    )

    def __init__(self, client, *, path, metadata, container_dispatch, queries=None):
        "This is not user-facing. Use ClientCatalog.from_uri."
        self._client = client
        self._metadata = metadata
        self.container_dispatch = collections.ChainMap(
            container_dispatch or {},
            self.DEFAULT_CONTAINER_DISPATCH,
        )
        if isinstance(path, str):
            raise ValueError("path is expected to be a list of segments")
        # Stash *immutable* copies just to be safe.
        self._path = tuple(path or [])
        self._queries = tuple(queries or [])
        self._queries_as_params = _queries_to_params(*self._queries)
        self.keys_indexer = IndexCallable(self._keys_indexer)
        self.items_indexer = IndexCallable(self._items_indexer)
        self.values_indexer = IndexCallable(self._values_indexer)

    @classmethod
    def from_uri(cls, uri, token, container_dispatch=None):
        client = httpx.Client(
            base_url=uri.rstrip("/"),
            headers={"X-Access-Token": token},
        )
        response = client.get("/metadata/")
        response.raise_for_status()
        metadata = response.json()["data"]["attributes"]["metadata"]
        return cls(
            client, path=[], metadata=metadata, container_dispatch=container_dispatch
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
        "Return type(self) or a container class (e.g. ClientArraySource)."
        if item["type"] == "catalog":
            cls = type(self)
        else:
            cls = self.container_dispatch[item["attributes"]["container"]]
        return cls

    def __len__(self):
        response = self._client.get(
            f"/search/{'/'.join(self._path)}",
            params={"fields": "", **self._queries_as_params},
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
                params={"fields": "", **self._queries_as_params},
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
                "fields": ["metadata", "container"],
                **_queries_to_params(KeyLookup(key)),
                **self._queries_as_params,
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
        )

    def items(self):
        # The base implementation would use __iter__ and __getitem__, making
        # one HTTP request per item. Pull pages instead.
        next_page_url = f"/search/{'/'.join(self._path)}"
        while next_page_url is not None:
            response = self._client.get(
                next_page_url,
                params={"fields": ["metadata", "container"], **self._queries_as_params},
            )
            response.raise_for_status()
            for item in response.json()["data"]:
                cls = self._get_class(item)
                yield cls(
                    self._client,
                    path=self.path + [item["id"]],
                    metadata=item["attributes"]["metadata"],
                    container_dispatch=self.container_dispatch,
                )
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
                params={"fields": "", **self._queries_as_params},
            )
            response.raise_for_status()
            for item in response.json()["data"]:
                if stop is not None and next(item_counter) == stop:
                    break
                yield item["id"]
            next_page_url = response.json()["links"]["next"]

    def _items_slice(self, start, stop):
        next_page_url = f"/search/{'/'.join(self._path)}?page[offset]={start}"
        item_counter = itertools.count(start)
        while next_page_url is not None:
            response = self._client.get(
                next_page_url,
                params={"fields": ["metadata", "container"], **self._queries_as_params},
            )
            response.raise_for_status()
            for item in response.json()["data"]:
                if stop is not None and next(item_counter) == stop:
                    break
                key = item["id"]
                cls = self._get_class(item)
                yield key, cls(
                    self._client,
                    path=self._path + (item["id"],),
                    metadata=item["attributes"]["metadata"],
                    container_dispatch=self.container_dispatch,
                )
            next_page_url = response.json()["links"]["next"]

    def _item_by_index(self, index):
        if index >= len(self):
            raise IndexError(f"index {index} out of range for length {len(self)}")
        url = f"/search/{'/'.join(self._path)}?page[offset]={index}&page[limit]=1"
        response = self._client.get(
            url, params={"fields": ["metadata", "container"], **self._queries_as_params}
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
        )
        return (key, value)

    def search(self, query):
        return type(self)(
            client=self._client,
            path=self._path,
            queries=self._queries + (query,),
            metadata=self._metadata,
            container_dispatch=self.container_dispatch,
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
