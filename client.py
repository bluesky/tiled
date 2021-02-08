import collections.abc
import itertools

import httpx

from queries import DictView
from in_memory_catalog import (
    CatalogKeysSequence,
    CatalogValuesSequence,
    CatalogItemsSequence,
)


class ClientCatalog(collections.abc.Mapping):

    # This maps the (__module__, __qualname__) sent by the server to a
    # client-side object. It is populated below, in the module scope, so as to
    # reference ClientCatalog itself.
    DEFAULT_DISPATCH = {}

    def __init__(self, client, *, path, metadata, dispatch):
        self._client = client
        self._metadata = metadata
        self.dispatch = self.DEFAULT_DISPATCH.copy()
        self.dispatch.update(dispatch or {})
        if isinstance(path, str):
            raise ValueError("path is expected to be a list of segments")
        self._path = path or []

    @classmethod
    def from_uri(cls, uri, dispatch=None):
        client = httpx.Client(base_url=uri.rstrip("/"))
        response = client.get(f"/entry/metadata/")
        metadata = response.json()["data"]["attributes"]["metadata"]
        return cls(client, path=[], metadata=metadata, dispatch=dispatch)

    @property
    def metadata(self):
        "Metadata about this Catalog."
        # Ensure this is immutable (at the top level) to help the user avoid
        # getting the wrong impression that editing this would update anything
        # persistent.
        return DictView(self._metadata)

    def __len__(self):
        response = self._client.get(f"/catalogs/entries/count/{'/'.join(self._path)}")
        return response.json()["data"]["attributes"]["count"]

    def __iter__(self):
        next_page_url = f"/catalogs/entries/keys/{'/'.join(self._path)}"
        while next_page_url is not None:
            response = self._client.get(next_page_url)
            for item in response.json()["data"]:
                yield item["attributes"]["key"]
            next_page_url = response.json()["links"]["next"]

    def __getitem__(self, key):
        response = self._client.get(f"/entry/metadata/{'/'.join(self._path + [key])}")
        data = response.json()["data"]
        dispatch_on = (data["meta"]["__module__"], data["meta"]["__qualname__"])
        cls = self.dispatch[dispatch_on]
        return cls(
            client=self._client,
            path=data["id"].split("/"),
            metadata=data["attributes"]["metadata"],
        )

    def items(self):
        # The base implementation would use __iter__ and __getitem__, making
        # one HTTP request per item. Pull pages instead.
        next_page_url = f"/catalogs/entries/metadata/{'/'.join(self._path)}"
        while next_page_url is not None:
            response = self._client.get(next_page_url)
            for item in response.json()["data"]:
                dispatch_on = (item["meta"]["__module__"], item["meta"]["__qualname__"])
                cls = self.dispatch[dispatch_on]
                yield cls(
                    self._client,
                    path=item["id"].split("/"),
                    metadata=item["attributes"]["metadata"],
                    dispatch=self.dispatch,
                )
            next_page_url = response.json()["links"]["next"]

    def values(self):
        # The base implementation would use __iter__ and __getitem__, making
        # one HTTP request per item. Pull pages instead.
        for _, value in self.items():
            yield value

    def _keys_slice(self, start, stop):
        next_page_url = (
            f"/catalogs/entries/keys/{'/'.join(self._path)}?page[offset]={start}"
        )
        item_counter = itertools.count(start)
        while next_page_url is not None:
            response = self._client.get(next_page_url)
            for item in response.json()["data"]:
                if stop is not None and next(item_counter) == stop:
                    break
                yield item["attributes"]["key"]
            next_page_url = response.json()["links"]["next"]

    def _items_slice(self, start, stop):
        next_page_url = (
            f"/catalogs/entries/metadata/{'/'.join(self._path)}?page[offset]={start}"
        )
        item_counter = itertools.count(start)
        while next_page_url is not None:
            response = self._client.get(next_page_url)
            for item in response.json()["data"]:
                dispatch_on = (item["meta"]["__module__"], item["meta"]["__qualname__"])
                cls = self.dispatch[dispatch_on]
                if stop is not None and next(item_counter) == stop:
                    break
                yield cls(
                    self._client,
                    path=item["id"].split("/"),
                    metadata=item["attributes"]["metadata"],
                    dispatch=self.dispatch,
                )
            next_page_url = response.json()["links"]["next"]

    def _item_by_index(self, index):
        if index >= len(self):
            raise IndexError(f"index {index} out of range for length {len(self)}")
        url = f"/catalogs/entries/metadata/{'/'.join(self._path)}?page[offset]={index}&page[limit]=1"
        response = self._client.get(url)
        (item,) = response.json()["data"]
        key = item["attributes"]["key"]
        dispatch_on = (item["meta"]["__module__"], item["meta"]["__qualname__"])
        cls = self.dispatch[dispatch_on]
        value = cls(
            self._client,
            path=item["id"].split("/"),
            metadata=item["attributes"]["metadata"],
            dispatch=self.dispatch,
        )
        return (key, value)

    @property
    def keys_indexer(self):
        return CatalogKeysSequence(self)

    @property
    def items_indexer(self):
        return CatalogItemsSequence(self)

    @property
    def values_indexer(self):
        return CatalogValuesSequence(self)


class ClientArraySource:
    ...


ClientCatalog.DEFAULT_DISPATCH.update(
    {
        ("in_memory_catalog", "Catalog"): ClientCatalog,
        ("datasources", "ArraySource"): ClientArraySource,
    }
)
