import collections.abc
import itertools

import httpx


class ClientCatalog(collections.abc.Mapping):
    def __init__(self, client, path=None, dispatch=None, offset=0, limit=None):
        self.dispatch = DEFAULT_DISPATCH.copy()
        self.dispatch.update(dispatch or {})
        self._client = client
        self._path = path or []
        self._offset = offset
        self._limit = limit
        self._index_accessor = _IndexAccessor(
            client=self._client,
            path=path,
            dispatch=dispatch,
            offset=offset,
            limit=limit,
        )

    @classmethod
    def from_uri(cls, uri, dispatch=None):
        client = httpx.Client(base_url=uri)
        return cls(client, dispatch=dispatch)

    def __len__(self):
        just_the_meta = self._client.get(
            f"/catalogs/keys/{'/'.join(self._path)}", params={"page[limit]": 0}
        )
        return just_the_meta["meta"]["count"]

    def __iter__(self):
        next_page_url = (
            f"/catalogs/keys/{'/'.join(self._path)}?page[offset]={self._offset}"
        )
        item_counter = itertools.count(1)
        while next_page_url is not None:
            response = self._client.get(next_page_url)
            for item in response.json()["data"]:
                yield item["attributes"]["key"]
                if self._limit is not None and next(item_counter) == self._limit:
                    break
            next_page_url = response.json()["links"]["next"]

    def __getitem__(self, key):
        ...

    def items(self):
        # The base implementation would use __iter__ and __getitem__, making
        # one HTTP request per item. Pull pages instead.
        next_page_url = (
            f"/catalogs/keys/{'/'.join(self._path)}?page[offset]={self._offset}"
        )
        item_counter = itertools.count(1)
        while next_page_url is not None:
            response = self._client.get(next_page_url)
            for item in response.json()["data"]:
                dispatch_on = (item["meta"]["__module__"], item["meta"]["__qualname__"])
                yield self.dispatch[dispatch_on](self._client, item["id"])
                if self._limit is not None and next(item_counter) == self._limit:
                    break
            next_page_url = response.json()["links"]["next"]

    def values(self):
        # The base implementation would use __iter__ and __getitem__, making
        # one HTTP request per item. Pull pages instead.
        for _, value in self.items():
            yield value

    @property
    def index(self):
        return self._index_accessor


class _IndexAccessor:
    "Internal object used by ClientCatalog."

    def __init__(self, *, client, path, dispatch, offset, limit):
        self._client = client
        self._path = path
        self._dispatch = dispatch
        self._offset = offset
        self._limit = limit

    def __getitem__(self, i):
        if isinstance(i, int):
            # TODO
            out = ...
        elif isinstance(i, slice):
            if not ((i.step is None) or (i.step == 1)):
                raise NotImplementedError
            if i.start is None:
                offset = self._offset
            elif i.start < 0:
                raise NotImplementedError
            else:
                offset = self._offset + i.start
            if i.stop is None:
                limit = self._limit
            elif i.stop < 0:
                raise NotImplementedError
            else:
                if self._limit is None:
                    limit = offset + i.stop
                else:
                    limit = min(self._limit, offset + i.stop)
            out = ClientCatalog(
                client=self._client,
                offset=offset,
                limit=limit,
                dispatch=self._dispatch,
                path=self._path,
            )
        else:
            raise TypeError("Catalog index must be integer or slice.")
        return out


class ClientArraySource:
    ...


DEFAULT_DISPATCH = {
    ("in_memory_catalog", "Catalog"): ClientCatalog,
    ("datasources", "ArraySource"): ClientArraySource,
}
