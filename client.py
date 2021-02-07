import collections.abc
import itertools
import httpx


class ClientCatalog(collections.abc.Mapping):
    def __init__(self, uri, path=None, dispatch=None):
        self.dispatch = DEFAULT_DISPATCH.copy()
        self.dispatch.update(dispatch or {})
        if isinstance(uri, httpx.Client):
            self._client = uri
        else:
            self._client = httpx.Client(base_url=uri)
        if isinstance(path, str):
            path = path.split("/")
        self._path = path or []

    def __len__(self):
        just_the_meta = self._client.get(
            f"/catalogs/keys/{'/'.join(self._path)}", params={"page[limit]": 0}
        )
        return just_the_meta["meta"]["count"]

    def __iter__(self):
        next_page_url = f"/catalogs/keys/{'/'.join(self._path)}"
        while next_page_url is not None:
            response = self._client.get(next_page_url)
            for item in response.json()["data"]:
                yield item["attributes"]["key"]
            next_page_url = response.json()["links"]["next"]

    def __getitem__(self):
        ...

    def items(self):
        # The base implementation would use __iter__ and __getitem__, making
        # one HTTP request per item. Pull pages instead.
        next_page_url = f"/catalogs/entries/{'/'.join(self._path)}"
        while next_page_url is not None:
            response = self._client.get(next_page_url)
            for item in response.json()["data"]:
                dispatch_on = (item["meta"]["__module__"], item["meta"]["__qualname__"])
                yield self.dispatch[dispatch_on](self._client, item["id"])
            next_page_url = response.json()["links"]["next"]

    @property
    def index(self):
        return self._index_accessor


class _IndexAccessor:
    "Internal object used by Catalog."

    def __init__(self, entries, out_type):
        self._entries = entries
        self._out_type = out_type

    def __getitem__(self, /, i):
        if isinstance(i, int):
            if i >= len(self._entries):
                raise IndexError("Catalog index out of range.")
            out = next(itertools.islice(self._entries.values(), i, 1 + i))
        elif isinstance(i, slice):
            out = self._out_type(
                dict(itertools.islice(self._entries.items(), i.start, i.stop, i.step))
            )
        else:
            raise TypeError("Catalog index must be integer or slice.")
        return out


DEFAULT_DISPATCH = {
    ("in_memory_catalog", "Catalog"): ClientCatalog,
}
