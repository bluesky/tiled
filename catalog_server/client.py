import collections.abc
import itertools

import httpx
import dask.array
import numpy

from .models import LabeledCatalogQuery, DataSourceStructure
from .queries import DictView
from .in_memory_catalog import (
    CatalogKeysSequence,
    CatalogValuesSequence,
    CatalogItemsSequence,
)


class ClientCatalog(collections.abc.Mapping):

    # This maps the (__module__, __qualname__) sent by the server to a
    # client-side object. It is populated below, in the module scope, so as to
    # reference ClientCatalog itself.
    DEFAULT_DISPATCH = {}

    def __init__(self, client, *, path, metadata, dispatch, queries=None):
        "This is not user-facing. Use ClientCatalog.from_uri."
        self._client = client
        self._metadata = metadata
        self.dispatch = self.DEFAULT_DISPATCH.copy()
        self.dispatch.update(dispatch or {})
        if isinstance(path, str):
            raise ValueError("path is expected to be a list of segments")
        self._path = path or []
        self._queries = queries or []

    @classmethod
    def from_uri(cls, uri, dispatch=None):
        client = httpx.Client(base_url=uri.rstrip("/"))
        response = client.get("/metadata/")
        response.raise_for_status()
        metadata = response.json()["data"]["attributes"]["metadata"]
        return cls(client, path=[], metadata=metadata, dispatch=dispatch)

    def __repr__(self):
        # TODO When the number of items is very large, show only a subset.
        # That is, do not let repr(self) trigger a large number of HTTP
        # requests paginating through all the results.
        return f"<{type(self).__name__}({set(self)!r})>"

    @property
    def metadata(self):
        "Metadata about this Catalog."
        # Ensure this is immutable (at the top level) to help the user avoid
        # getting the wrong impression that editing this would update anything
        # persistent.
        return DictView(self._metadata)

    def __len__(self):
        response = self._client.get(
            f"/metadata/{'/'.join(self._path)}", params={"fields": "count"}
        )
        response.raise_for_status()
        return response.json()["data"]["attributes"]["count"]

    def __iter__(self):
        next_page_url = f"/search/{'/'.join(self._path)}"
        while next_page_url is not None:
            response = self._client.post(
                next_page_url,
                params={"fields": []},
                json=self._queries,
            )
            response.raise_for_status()
            for item in response.json()["data"]:
                yield item["id"]
            next_page_url = response.json()["links"]["next"]

    def __getitem__(self, key):
        response = self._client.get(
            f"/metadata/{'/'.join(self._path + [key])}", params={"fields": "metadata"}
        )
        response.raise_for_status()
        data = response.json()["data"]
        dispatch_on = (data["meta"]["__module__"], data["meta"]["__qualname__"])
        cls = self.dispatch[dispatch_on]
        return cls(
            client=self._client,
            path=self._path + [data["id"]],
            metadata=data["attributes"]["metadata"],
            dispatch=self.dispatch,
        )

    def items(self):
        # The base implementation would use __iter__ and __getitem__, making
        # one HTTP request per item. Pull pages instead.
        next_page_url = f"/search/{'/'.join(self._path)}"
        while next_page_url is not None:
            response = self._client.post(
                next_page_url,
                params={"fields": "metadata"},
                json=self._queries,
            )
            response.raise_for_status()
            for item in response.json()["data"]:
                dispatch_on = (item["meta"]["__module__"], item["meta"]["__qualname__"])
                cls = self.dispatch[dispatch_on]
                yield cls(
                    self._client,
                    path=self.path + [item["id"]],
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
        next_page_url = f"/search/{'/'.join(self._path)}?page[offset]={start}"
        item_counter = itertools.count(start)
        while next_page_url is not None:
            response = self._client.post(
                next_page_url,
                params={"fields": []},
                json=self._queries,
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
            response = self._client.post(
                next_page_url,
                params={"fields": "metadata"},
                json=self._queries,
            )
            response.raise_for_status()
            for item in response.json()["data"]:
                key = item["id"]
                dispatch_on = (item["meta"]["__module__"], item["meta"]["__qualname__"])
                cls = self.dispatch[dispatch_on]
                if stop is not None and next(item_counter) == stop:
                    break
                yield key, cls(
                    self._client,
                    path=self._path + [item["id"]],
                    metadata=item["attributes"]["metadata"],
                    dispatch=self.dispatch,
                )
            next_page_url = response.json()["links"]["next"]

    def _item_by_index(self, index):
        if index >= len(self):
            raise IndexError(f"index {index} out of range for length {len(self)}")
        url = f"/search/{'/'.join(self._path)}?page[offset]={index}&page[limit]=1"
        response = self._client.post(
            url, params={"fields": "metadata"}, json=self._queries
        )
        response.raise_for_status()
        (item,) = response.json()["data"]
        key = item["id"]
        dispatch_on = (item["meta"]["__module__"], item["meta"]["__qualname__"])
        cls = self.dispatch[dispatch_on]
        value = cls(
            self._client,
            path=self._path + [item["id"]],
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

    def search(self, query):
        query_as_json = LabeledCatalogQuery.from_dataclass(query).dict()
        return type(self)(
            client=self._client,
            path=self._path,
            queries=self._queries + [query_as_json],
            metadata=self._metadata,
            dispatch=self.dispatch,
        )


class ClientArraySource:
    def __init__(self, client, metadata, path, dispatch):
        self._client = client
        self._metadata = metadata
        self._path = path

    @property
    def metadata(self):
        "Metadata about this Catalog."
        # Ensure this is immutable (at the top level) to help the user avoid
        # getting the wrong impression that editing this would update anything
        # persistent.
        return DictView(self._metadata)

    def describe(self):
        response = self._client.get(
            f"/metadata/{'/'.join(self._path)}", params={"fields": "structure"}
        )
        response.raise_for_status()
        result = response.json()["data"]["attributes"]["structure"]
        return DataSourceStructure(**result)

    def _get_block(self, block, dtype, shape):
        """
        Fetch the data for one block in a chunked (dask) array.
        """
        response = self._client.post(
            f"/blob/array/{'/'.join(self._path)}",
            headers={"Accept": "application/octet-stream"},
            json=block,
        )
        response.raise_for_status()
        return numpy.frombuffer(response.content, dtype=dtype).reshape(shape)

    def read(self):
        structure = self.describe()
        shape = structure.shape
        dtype = structure.dtype.to_numpy_dtype()
        # Build a client-side dask array whose chunks pull from a server-side
        # dask array.
        name = "remote-dask-array-{self._client.base_url!s}{'/'.join(self._path)}"
        chunks = structure.chunks
        # Count the number of blocks along each axis.
        num_blocks = (range(len(n)) for n in chunks)
        # Loop over each block index --- e.g. (0, 0), (0, 1), (0, 2) .... ---
        # and build a dask task encoding the method for fetching its data from
        # the server.
        dask_tasks = {
            (name,)
            + block: (
                self._get_block,
                block,
                dtype,
                tuple(chunks[dim][i] for dim, i in enumerate(block)),
            )
            for block in itertools.product(*num_blocks)
        }
        return dask.array.Array(
            dask=dask_tasks,
            name=name,
            chunks=chunks,
            dtype=dtype,
            shape=shape,
        )


ClientCatalog.DEFAULT_DISPATCH.update(
    {
        ("in_memory_catalog", "Catalog"): ClientCatalog,
        ("datasources", "ArraySource"): ClientArraySource,
    }
)
