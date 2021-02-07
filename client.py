import collections.abc

from queries import DictView
import httpx


class ClientCatalog(collections.abc.Mapping):

    # This maps the (__module__, __qualname__) sent by the server to a
    # client-side object. It is populated below, in the module scope, so as to
    # reference ClientCatalog itself.
    DEFAULT_DISPATCH = {}

    def __init__(self, client, *, path=None, metadata=None, dispatch=None):
        self._client = client
        self._metadata = metadata
        self.dispatch = self.DEFAULT_DISPATCH.copy()
        self.dispatch.update(dispatch or {})
        if isinstance(path, str):
            raise ValueError("path is expected to be a list of segments")
        self._path = path or []
        self._index_accessor = _IndexAccessor(
            client=self._client,
            path=path,
            dispatch=dispatch,
        )

    @classmethod
    def from_uri(cls, uri, dispatch=None):
        client = httpx.Client(base_url=uri.rstrip("/"))
        return cls(client, dispatch=dispatch)

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

    @property
    def index(self):
        return self._index_accessor


class _IndexAccessor:
    "Internal object used by ClientCatalog."

    def __init__(self, *, client, path, dispatch):
        self._client = client
        self._path = path
        self._dispatch = dispatch

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
            raise TypeError(f"{type(self).__name__} index must be integer or slice.")
        return out


class ClientArraySource:
    ...


ClientCatalog.DEFAULT_DISPATCH.update(
    {
        ("in_memory_catalog", "Catalog"): ClientCatalog,
        ("datasources", "ArraySource"): ClientArraySource,
    }
)
