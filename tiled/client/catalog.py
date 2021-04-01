import asyncio
import atexit
import collections
import collections.abc
from dataclasses import fields
import functools
import importlib
import itertools
import warnings

import entrypoints
import httpx

from ..query_registration import query_type_to_name
from ..queries import KeyLookup
from ..utils import (
    DictView,
    OneShotCachedMap,
)
from .utils import get_json_with_cache
from ..catalogs.utils import (
    catalog_repr,
    IndexersMixin,
    UNCHANGED,
)


class Catalog(collections.abc.Mapping, IndexersMixin):

    # This maps the structure_family sent by the server to a client-side object that
    # can interpret the structure_family's structure and content. OneShotCachedMap is used to
    # defer imports.
    DEFAULT_STRUCTURE_CLIENT_DISPATCH = {
        "numpy": OneShotCachedMap(
            {
                "array": lambda: importlib.import_module(
                    "..array", Catalog.__module__
                ).ClientArrayAdapter,
                "dataframe": lambda: importlib.import_module(
                    "..dataframe", Catalog.__module__
                ).ClientDataFrameAdapter,
                "variable": lambda: importlib.import_module(
                    "..xarray", Catalog.__module__
                ).ClientVariableAdapter,
                "data_array": lambda: importlib.import_module(
                    "..xarray", Catalog.__module__
                ).ClientDataArrayAdapter,
                "dataset": lambda: importlib.import_module(
                    "..xarray", Catalog.__module__
                ).ClientDatasetAdapter,
            }
        ),
        "dask": OneShotCachedMap(
            {
                "array": lambda: importlib.import_module(
                    "..array", Catalog.__module__
                ).ClientDaskArrayAdapter,
                "dataframe": lambda: importlib.import_module(
                    "..dataframe", Catalog.__module__
                ).ClientDaskDataFrameAdapter,
                "variable": lambda: importlib.import_module(
                    "..xarray", Catalog.__module__
                ).ClientDaskVariableAdapter,
                "data_array": lambda: importlib.import_module(
                    "..xarray", Catalog.__module__
                ).ClientDaskDataArrayAdapter,
                "dataset": lambda: importlib.import_module(
                    "..xarray", Catalog.__module__
                ).ClientDaskDatasetAdapter,
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

        This is called once automatically the first time Catalog.from_uri
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

    @classmethod
    def from_uri(
        cls,
        uri,
        structure_clients="numpy",
        *,
        cache=None,
        offline=False,
        token=None,
        special_clients=None,
    ):
        """
        Connect to a Catalog on a local or remote server.

        Parameters
        ----------
        uri : str
            e.g. "http://localhost:8000"
        structure_clients : str or dict
            Use "dask" for delayed data loading and "numpy" for immediate
            in-memory structures (e.g. normal numpy arrays, pandas
            DataFrames). For advanced use, provide dict mapping
            structure_family names ("array", "dataframe", "variable",
            "data_array", "dataset") to client objects. See
            ``Catalog.DEFAULT_STRUCTURE_CLIENT_DISPATCH``.
        cache : Cache, optional
        offline : bool, optional
            False by default. If True, rely on cache only.
        special_clients : dict
            Advanced: Map client_type_hint from the server to special client
            catalog objects. See also
            ``Catalog.discover_special_clients()`` and
            ``Catalog.DEFAULT_SPECIAL_CLIENT_DISPATCH``.
        """
        headers = {}
        if token is not None:
            headers["X-Access-Token"] = token
        client = httpx.Client(
            base_url=uri.rstrip("/"),
            headers=headers,
        )
        return cls.from_client(
            client,
            cache=cache,
            offline=offline,
            structure_clients=structure_clients,
            special_clients=special_clients,
        )

    @classmethod
    def direct(
        cls,
        catalog,
        structure_clients="numpy",
        *,
        token=None,
        special_clients=None,
    ):
        """
        Connect to a Catalog directly, running the app in this same process.

        NOTE: This is experimental. It may need to be re-designed or even removed.

        In this configuration, we are using the server, but we are communicating
        with it directly within this process, not over a local network. It is
        generally faster.

        Specifically, we are using HTTP over ASGI rather than HTTP over TCP.
        There are no sockets or network-related syscalls.

        Parameters
        ----------
        client : httpx.Client
            Should be pre-configured with a base_url and any auth-related headers.
        structure_clients : str or dict
            Use "dask" for delayed data loading and "numpy" for immediate
            in-memory structures (e.g. normal numpy arrays, pandas
            DataFrames). For advanced use, provide dict mapping
            structure_family names ("array", "dataframe", "variable",
            "data_array", "dataset") to client objects. See
            ``Catalog.DEFAULT_STRUCTURE_CLIENT_DISPATCH``.
        special_clients : dict
            Advanced: Map client_type_hint from the server to special client
            catalog objects. See also
            ``Catalog.discover_special_clients()`` and
            ``Catalog.DEFAULT_SPECIAL_CLIENT_DISPATCH``.
        """
        from ..server.main import app, get_settings

        @functools.lru_cache(1)
        def override_settings():
            settings = get_settings()
            settings.catalog = catalog
            return settings

        app.dependency_overrides[get_settings] = override_settings
        # Note: This is important. The Tiled server routes are defined lazily on startup.
        asyncio.run(app.router.startup())

        headers = {}
        if token is not None:
            headers["X-Access-Token"] = token
        # Only an AsyncClient can be used over ASGI.
        # We wrap all the async methods in a call to asyncio.run(...).
        # Someday we should explore asynchronous Tiled Client objects.
        client = httpx.AsyncClient(
            base_url="http://local-tiled-app",
            headers=headers,
            app=app,
        )
        # TODO How to close the httpx.AsyncClient more cleanly?
        atexit.register(asyncio.run, client.aclose())

        return cls.from_client(
            client,
            structure_clients=structure_clients,
            # The cache and "offline" mode do not make much sense when we have an
            # in-process connection. It's also not clear what URL we would use for the cache
            # even if we wanted to.... but it might be worth rethinking this someday.
            cache=None,
            offline=False,
            special_clients=special_clients,
        )

    @classmethod
    def from_client(
        cls,
        client,
        structure_clients="numpy",
        *,
        cache=None,
        offline=False,
        special_clients=None,
    ):
        """
        Advanced: Connect to a Catalog using a custom instance of httpx.Client or httpx.AsyncClient.

        Parameters
        ----------
        client : httpx.Client
            Should be pre-configured with a base_url and any auth-related headers.
        structure_clients : str or dict
            Use "dask" for delayed data loading and "numpy" for immediate
            in-memory structures (e.g. normal numpy arrays, pandas
            DataFrames). For advanced use, provide dict mapping
            structure_family names ("array", "dataframe", "variable",
            "data_array", "dataset") to client objects. See
            ``Catalog.DEFAULT_STRUCTURE_CLIENT_DISPATCH``.
        cache : Cache, optional
        offline : bool, optional
            False by default. If True, rely on cache only.
        special_clients : dict
            Advanced: Map client_type_hint from the server to special client
            catalog objects. See also
            ``Catalog.discover_special_clients()`` and
            ``Catalog.DEFAULT_SPECIAL_CLIENT_DISPATCH``.
        """
        # Interpret structure_clients="numpy" and structure_clients="dask" shortcuts.
        if isinstance(structure_clients, str):
            structure_clients = cls.DEFAULT_STRUCTURE_CLIENT_DISPATCH[structure_clients]
        # Do entrypoint discovery if it hasn't yet been done.
        if cls.DEFAULT_SPECIAL_CLIENT_DISPATCH is None:
            cls.discover_special_clients()
        special_clients = collections.ChainMap(
            special_clients or {},
            cls.DEFAULT_SPECIAL_CLIENT_DISPATCH,
        )
        content = get_json_with_cache(cache, offline, client, "/metadata/")
        metadata = content["data"]["attributes"]["metadata"]
        return cls(
            client,
            offline=offline,
            path=[],
            metadata=metadata,
            structure_clients=structure_clients,
            cache=cache,
            special_clients=special_clients,
            root_client_type=cls,
        )

    def __init__(
        self,
        client,
        *,
        offline,
        path,
        metadata,
        root_client_type,
        structure_clients,
        cache,
        special_clients,
        params=None,
        queries=None,
    ):
        "This is not user-facing. Use Catalog.from_uri."

        self._client = client
        self._offline = offline
        self._metadata = metadata
        self._cache = cache
        self.structure_clients = structure_clients
        self.special_clients = special_clients
        self._root_client_type = root_client_type
        if isinstance(path, str):
            raise ValueError("path is expected to be a list of segments")
        # Stash *immutable* copies just to be safe.
        self._path = tuple(path or [])
        self._queries = tuple(queries or [])
        self._queries_as_params = _queries_to_params(*self._queries)
        self._params = params or {}
        super().__init__()

    def __repr__(self):
        # Display up to the first N keys to avoid making a giant service
        # request. Use _keys_slicer because it is unauthenticated.
        N = 10
        return catalog_repr(self, self._keys_slice(0, N))

    @property
    def metadata(self):
        "Metadata about this Catalog."
        # Ensure this is immutable (at the top level) to help the user avoid
        # getting the wrong impression that editing this would update anything
        # persistent.
        return DictView(self._metadata)

    def touch(self):
        """
        Access all the data in this Catalog.

        This causes it to be cached if the client is configured with a cache.
        """
        get_json_with_cache(self._cache, self._offline, self._client, "/metadata/")
        repr(self)
        for key in self:
            entry = self[key]
            entry.touch()

    def _get_class(self, item):
        # The basic structure of the response is either one of the structure_clients
        # we know about or a sub-Catalog.
        if item["type"] == "reader":
            structure_family = item["attributes"]["structure_family"]
            try:
                return self.structure_clients[structure_family]
            except KeyError:
                raise UnknownStructureFamily(structure_family) from None
        # If a catalog, server can hint that we should use a special variant
        # that might have a special __repr__, or extra methods for usability,
        # etc.
        client_type_hint = item["attributes"].get("client_type_hint")
        if client_type_hint is not None:
            class_ = self.special_clients.get(client_type_hint)
            if class_ is None:
                warnings.warn(
                    "The server suggested to use a special client with the "
                    f"hint {client_type_hint!r} but nothing matching the "
                    "description could be discovered in the current software "
                    "environment. We will fall back back to a default that "
                    "should be functional but may lack some usability "
                    "features."
                )
            else:
                return class_
        # This is generally just Catalog, but if the original
        # user-created catalog was a subclass of Catalog, this will
        # repsect that.
        return self._root_client_type

    def client_for_item(self, item, path, metadata):
        class_ = self._get_class(item)
        if item["type"] == "catalog":
            return class_(
                client=self._client,
                offline=self._offline,
                cache=self._cache,
                path=path,
                metadata=metadata,
                structure_clients=self.structure_clients,
                special_clients=self.special_clients,
                params=self._params,
                queries=None,
                root_client_type=self._root_client_type,
            )
        else:  # item["type"] == "reader"
            return class_(
                client=self._client,
                offline=self._offline,
                cache=self._cache,
                path=path,
                metadata=metadata,
                params=self._params,
            )

    def new_variation(
        self,
        *,
        offline=UNCHANGED,
        path=UNCHANGED,
        metadata=UNCHANGED,
        structure_clients=UNCHANGED,
        special_clients=UNCHANGED,
        cache=UNCHANGED,
        params=UNCHANGED,
        queries=UNCHANGED,
    ):
        """
        This is intended primarily for intenal use and use by subclasses.
        """
        if offline is UNCHANGED:
            offline = self._offline
        if path is UNCHANGED:
            path = self._path
        if metadata is UNCHANGED:
            metadata = self._metadata
        if structure_clients is UNCHANGED:
            structure_clients = self.structure_clients
        if special_clients is UNCHANGED:
            special_clients = self.special_clients
        if cache is UNCHANGED:
            cache = self._cache
        if params is UNCHANGED:
            params = self._params
        if queries is UNCHANGED:
            queries = self._queries
        return type(self)(
            client=self._client,
            cache=cache,
            offline=offline,
            path=path,
            metadata=metadata,
            structure_clients=structure_clients,
            special_clients=special_clients,
            params=params,
            queries=queries,
            root_client_type=self._root_client_type,
        )

    def __len__(self):
        content = get_json_with_cache(
            self._cache,
            self._offline,
            self._client,
            f"/search/{'/'.join(self._path)}",
            params={"fields": "", **self._queries_as_params, **self._params},
        )
        return content["meta"]["count"]

    def __length_hint__(self):
        # TODO The server should provide an estimated count.
        # https://www.python.org/dev/peps/pep-0424/
        return len(self)

    def __iter__(self):
        next_page_url = f"/search/{'/'.join(self._path)}"
        while next_page_url is not None:
            content = get_json_with_cache(
                self._cache,
                self._offline,
                self._client,
                next_page_url,
                params={"fields": "", **self._queries_as_params, **self._params},
            )
            for item in content["data"]:
                yield item["id"]
            next_page_url = content["links"]["next"]

    def __getitem__(self, key):
        # Lookup this key *within the search results* of this Catalog.
        content = get_json_with_cache(
            self._cache,
            self._offline,
            self._client,
            f"/search/{'/'.join(self._path )}",
            params={
                "fields": ["metadata", "structure_family", "client_type_hint"],
                **_queries_to_params(KeyLookup(key)),
                **self._queries_as_params,
                **self._params,
            },
        )
        data = content["data"]
        if not data:
            raise KeyError(key)
        assert (
            len(data) == 1
        ), "The key lookup query must never result more than one result."
        (item,) = data
        return self.client_for_item(
            item,
            path=self._path + (item["id"],),
            metadata=item["attributes"]["metadata"],
        )

    def items(self):
        # The base implementation would use __iter__ and __getitem__, making
        # one HTTP request per item. Pull pages instead.
        next_page_url = f"/search/{'/'.join(self._path)}"
        while next_page_url is not None:
            content = get_json_with_cache(
                self._cache,
                self._offline,
                self._client,
                next_page_url,
                params={
                    "fields": ["metadata", "structure_family", "client_type_hint"],
                    **self._queries_as_params,
                    **self._params,
                },
            )
            for item in content["data"]:
                key = item["id"]
                value = self.client_for_item(
                    item,
                    path=self._path + (item["id"],),
                    metadata=item["attributes"]["metadata"],
                )
                yield key, value
            next_page_url = content["links"]["next"]

    def values(self):
        # The base implementation would use __iter__ and __getitem__, making
        # one HTTP request per item. Pull pages instead.
        for _, value in self.items():
            yield value

    # The following three methods are used by IndexersMixin
    # to define keys_indexer, items_indexer, and values_indexer.

    def _keys_slice(self, start, stop):
        next_page_url = f"/search/{'/'.join(self._path)}?page[offset]={start}"
        item_counter = itertools.count(start)
        while next_page_url is not None:
            content = get_json_with_cache(
                self._cache,
                self._offline,
                self._client,
                next_page_url,
                params={"fields": "", **self._queries_as_params, **self._params},
            )
            for item in content["data"]:
                if stop is not None and next(item_counter) == stop:
                    return
                yield item["id"]
            next_page_url = content["links"]["next"]

    def _items_slice(self, start, stop):
        next_page_url = f"/search/{'/'.join(self._path)}?page[offset]={start}"
        item_counter = itertools.count(start)
        while next_page_url is not None:
            content = get_json_with_cache(
                self._cache,
                self._offline,
                self._client,
                next_page_url,
                params={
                    "fields": ["metadata", "structure_family", "client_type_hint"],
                    **self._queries_as_params,
                    **self._params,
                },
            )

            for item in content["data"]:
                if stop is not None and next(item_counter) == stop:
                    return
                key = item["id"]
                yield key, self.client_for_item(
                    item,
                    path=self._path + (item["id"],),
                    metadata=item["attributes"]["metadata"],
                )
            next_page_url = content["links"]["next"]

    def _item_by_index(self, index):
        if index >= len(self):
            raise IndexError(f"index {index} out of range for length {len(self)}")
        url = f"/search/{'/'.join(self._path)}?page[offset]={index}&page[limit]=1"
        content = get_json_with_cache(
            self._cache,
            self._offline,
            self._client,
            url,
            params={
                "fields": ["metadata", "structure_family", "client_type_hint"],
                **self._queries_as_params,
                **self._params,
            },
        )
        (item,) = content["data"]
        key = item["id"]
        value = self.client_for_item(
            item,
            path=self._path + (item["id"],),
            metadata=item["attributes"]["metadata"],
        )
        return (key, value)

    def search(self, query):
        return self.new_variation(
            queries=self._queries + (query,),
        )


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


class UnknownStructureFamily(KeyError):
    pass
