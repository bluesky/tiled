import collections
import collections.abc
from dataclasses import fields
import importlib
import itertools
import time
import warnings

import entrypoints

from ..query_registration import query_type_to_name
from ..queries import KeyLookup
from ..utils import (
    import_object,
    OneShotCachedMap,
    Sentinel,
)
from .authentication import DEFAULT_TOKEN_CACHE, reauthenticate_client
from .base import BaseClient
from .utils import (
    client_and_path_from_uri,
    client_from_catalog,
    NEEDS_INITIALIZATION,
)
from ..catalogs.utils import (
    catalog_repr,
    IndexersMixin,
    UNCHANGED,
)


class Catalog(BaseClient, collections.abc.Mapping, IndexersMixin):

    # This maps the structure_family sent by the server to a client-side object that
    # can interpret the structure_family's structure and content. OneShotCachedMap is used to
    # defer imports.
    DEFAULT_STRUCTURE_CLIENT_DISPATCH = {
        "numpy": OneShotCachedMap(
            {
                "array": lambda: importlib.import_module(
                    "..array", Catalog.__module__
                ).ArrayClient,
                "dataframe": lambda: importlib.import_module(
                    "..dataframe", Catalog.__module__
                ).DataFrameClient,
                "variable": lambda: importlib.import_module(
                    "..xarray", Catalog.__module__
                ).VariableClient,
                "data_array": lambda: importlib.import_module(
                    "..xarray", Catalog.__module__
                ).DataArrayClient,
                "dataset": lambda: importlib.import_module(
                    "..xarray", Catalog.__module__
                ).DatasetClient,
            }
        ),
        "dask": OneShotCachedMap(
            {
                "array": lambda: importlib.import_module(
                    "..array", Catalog.__module__
                ).DaskArrayClient,
                "dataframe": lambda: importlib.import_module(
                    "..dataframe", Catalog.__module__
                ).DaskDataFrameClient,
                "variable": lambda: importlib.import_module(
                    "..xarray", Catalog.__module__
                ).DaskVariableClient,
                "data_array": lambda: importlib.import_module(
                    "..xarray", Catalog.__module__
                ).DaskDataArrayClient,
                "dataset": lambda: importlib.import_module(
                    "..xarray", Catalog.__module__
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
    def from_client(cls, *args, **kwargs):
        warnings.warn(
            "The classmethod Catalog.from_client is deperecated and will be removed. "
            "Use the function tiled.client.from_client instead."
        )
        return from_client(*args, **kwargs)

    @classmethod
    def from_catalog(cls, *args, **kwargs):
        warnings.warn(
            "The classmethod Catalog.from_catalog is being considered "
            "for deperecation and may be removed. "
            "The function tiled.client.from_catalog may be used instead.",
            PendingDeprecationWarning,
        )
        return from_catalog(*args, **kwargs)

    @classmethod
    def from_uri(cls, *args, **kwargs):
        warnings.warn(
            "The classmethod Catalog.from_uri is being considered "
            "for deperecation and may be removed. "
            "The function tiled.client.from_uri may be used instead.",
            PendingDeprecationWarning,
        )
        return from_uri(*args, **kwargs)

    @classmethod
    def from_profile(cls, *args, **kwargs):
        warnings.warn(
            "The classmethod Catalog.from_profile is being considered "
            "for deperecation and may be removed. "
            "The function tiled.client.from_profile may be used instead.",
            PendingDeprecationWarning,
        )
        return from_profile(*args, **kwargs)

    @classmethod
    def from_config(cls, *args, **kwargs):
        warnings.warn(
            "The classmethod Catalog.from_config is being considered "
            "for deperecation and may be removed. "
            "The function tiled.client.from_config may be used instead.",
            PendingDeprecationWarning,
        )
        return from_profile(*args, **kwargs)

    def __init__(
        self,
        client,
        *,
        username,
        offline,
        path,
        item,
        metadata,
        root_client_type,
        structure_clients,
        cache,
        special_clients,
        token_cache,
        params=None,
        queries=None,
        sorting=None,
    ):
        "This is not user-facing. Use Catalog.from_uri."

        self.structure_clients = structure_clients
        self.special_clients = special_clients
        self._root_client_type = root_client_type
        self._queries = list(queries or [])
        self._queries_as_params = _queries_to_params(*self._queries)
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
            client=client,
            item=item,
            username=username,
            token_cache=token_cache,
            cache=cache,
            offline=offline,
            path=path,
            metadata=metadata,
            params=params,
        )
        if metadata is NEEDS_INITIALIZATION:
            # TO DO: This is a big wart, a side-effect refactor to support
            # refresh tokens. Needs rethinking.
            content = self._get_json_with_cache(f"/metadata/{'/'.join(path)}")
            item = content["data"]
            self._metadata.update(item["attributes"]["metadata"])
            self._item = item

    def __repr__(self):
        # Display up to the first N keys to avoid making a giant service
        # request. Use _keys_slicer because it is unauthenticated.
        N = 10
        return catalog_repr(self, self._keys_slice(0, N, direction=1))

    @property
    def sorting(self):
        """
        The current sorting of this Catalog

        Given as a list of tuples where the first entry is the sorting key
        and the second entry indicates ASCENDING (or 1) or DESCENDING (or -1).
        """
        return list(self._sorting)

    def touch(self):
        """
        Access all the data in this Catalog.

        This causes it to be cached if the client is configured with a cache.
        """
        self._get_json_with_cache(self.uri)
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

    def client_for_item(self, item, path, metadata, sorting):
        """
        Create an instance of the appropriate client class for an item.

        This is intended primarily for internal use and use by subclasses.
        """
        class_ = self._get_class(item)
        if item["type"] == "catalog":
            return class_(
                client=self._client,
                username=self._username,
                item=item,
                offline=self._offline,
                cache=self._cache,
                path=path,
                metadata=metadata,
                structure_clients=self.structure_clients,
                special_clients=self.special_clients,
                token_cache=self._token_cache,
                params=self._params,
                queries=None,
                sorting=sorting,
                root_client_type=self._root_client_type,
            )
        else:  # item["type"] == "reader"
            return class_(
                client=self._client,
                item=item,
                offline=self._offline,
                cache=self._cache,
                path=path,
                metadata=metadata,
                params=self._params,
                username=self._username,
                token_cache=self._token_cache,
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
        Create a copy of this Catalog, optionally varying some parameters.

        This is intended primarily for intenal use and use by subclasses.
        """
        if isinstance(structure_clients, str):
            structure_clients = Catalog.DEFAULT_STRUCTURE_CLIENT_DISPATCH[
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
            structure_clients=structure_clients,
            special_clients=special_clients,
            queries=queries,
            sorting=sorting,
            root_client_type=self._root_client_type,
            **kwargs,
        )

    def __len__(self):
        now = time.monotonic()
        if self._cached_len is not None:
            length, deadline = self._cached_len
            if now < deadline:
                # Used the cached value and do not make any request.
                return length
        content = self._get_json_with_cache(
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
            content = self._get_json_with_cache(
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
        # Lookup this key *within the search results* of this Catalog.
        content = self._get_json_with_cache(
            self.item["links"]["search"],
            params={
                "fields": ["metadata", "structure_family", "client_type_hint"],
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
        return self.client_for_item(
            item,
            path=self._path + (item["id"],),
            metadata=item["attributes"]["metadata"],
            sorting=item["attributes"].get("sorting"),
        )

    def items(self):
        # The base implementation would use __iter__ and __getitem__, making
        # one HTTP request per item. Pull pages instead.
        next_page_url = self.item["links"]["search"]
        while next_page_url is not None:
            content = self._get_json_with_cache(
                next_page_url,
                params={
                    "fields": ["metadata", "structure_family", "client_type_hint"],
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
                value = self.client_for_item(
                    item,
                    path=self._path + (item["id"],),
                    metadata=item["attributes"]["metadata"],
                    sorting=item["attributes"].get("sorting"),
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
            content = self._get_json_with_cache(
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
            content = self._get_json_with_cache(
                next_page_url,
                params={
                    "fields": ["metadata", "structure_family", "client_type_hint"],
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
                yield key, self.client_for_item(
                    item,
                    path=self._path + (item["id"],),
                    metadata=item["attributes"]["metadata"],
                    sorting=item["attributes"].get("sorting"),
                )
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
        content = self._get_json_with_cache(
            next_page_url,
            params={
                "fields": ["metadata", "structure_family", "client_type_hint"],
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
        value = self.client_for_item(
            item,
            path=self._path + (item["id"],),
            metadata=item["attributes"]["metadata"],
            sorting=item["attributes"].get("sorting"),
        )
        return (key, value)

    def search(self, query):
        """
        Make a Catalog with a subset of this Catalog's entries, filtered by query.

        Examples
        --------

        >>> from tiled.queries import FullText
        >>> catalog.search(FullText("hello"))
        """
        return self.new_variation(
            queries=self._queries + [query],
        )

    def sort(self, sorting):
        """
        Make a Catalog with the same entries but sorted according to `sorting`.

        Examples
        --------

        Sort by "color" in ascending order, and then by "height" in descending order.

        >>> from tiled.client import ASCENDING, DESCENDING
        >>> catalog.sort([("color", ASCENDING), ("height", DESCENDING)])

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
                    "Tab-completition is not supported on this particular Catalog "
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
        name = query_type_to_name[type(query)]
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


def from_uri(
    uri,
    structure_clients="numpy",
    *,
    cache=None,
    offline=False,
    username=None,
    token_cache=DEFAULT_TOKEN_CACHE,
    special_clients=None,
):
    """
    Connect to a Catalog on a local or remote server.

    Parameters
    ----------
    uri : str
        e.g. "http://localhost:8000"
    structure_clients : str or dict, optional
        Use "dask" for delayed data loading and "numpy" for immediate
        in-memory structures (e.g. normal numpy arrays, pandas
        DataFrames). For advanced use, provide dict mapping
        structure_family names ("array", "dataframe", "variable",
        "data_array", "dataset") to client objects. See
        ``Catalog.DEFAULT_STRUCTURE_CLIENT_DISPATCH``.
    cache : Cache, optional
    offline : bool, optional
        False by default. If True, rely on cache only.
    username : str, optional
        Username for authenticated access.
    token_cache : str, optional
        Path to directory for storing refresh tokens.
    special_clients : dict, optional
        Advanced: Map client_type_hint from the server to special client
        catalog objects. See also
        ``Catalog.discover_special_clients()`` and
        ``Catalog.DEFAULT_SPECIAL_CLIENT_DISPATCH``.
    """
    client, path = client_and_path_from_uri(uri)
    return from_client(
        client,
        structure_clients=structure_clients,
        username=username,
        path=path,
        cache=cache,
        offline=offline,
        special_clients=special_clients,
        token_cache=token_cache,
    )


def from_catalog(
    catalog,
    authentication=None,
    server_settings=None,
    structure_clients="numpy",
    *,
    cache=None,
    offline=False,
    username=None,
    special_clients=None,
    token_cache=DEFAULT_TOKEN_CACHE,
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
    catalog : Catalog
    authentication : dict, optional
        Dict of authentication configuration.
    username : str, optional
        Username for authenticated access.
    structure_clients : str or dict, optional
        Use "dask" for delayed data loading and "numpy" for immediate
        in-memory structures (e.g. normal numpy arrays, pandas
        DataFrames). For advanced use, provide dict mapping
        structure_family names ("array", "dataframe", "variable",
        "data_array", "dataset") to client objects. See
        ``Catalog.DEFAULT_STRUCTURE_CLIENT_DISPATCH``.
    cache : Cache, optional
    offline : bool, optional
        False by default. If True, rely on cache only.
    special_clients : dict, optional
        Advanced: Map client_type_hint from the server to special client
        catalog objects. See also
        ``Catalog.discover_special_clients()`` and
        ``Catalog.DEFAULT_SPECIAL_CLIENT_DISPATCH``.
    token_cache : str, optional
        Path to directory for storing refresh tokens.
    """
    client = client_from_catalog(
        catalog=catalog,
        authentication=authentication,
        server_settings=server_settings,
    )
    return from_client(
        client,
        structure_clients=structure_clients,
        username=username,
        # The cache and "offline" mode do not make much sense when we have an
        # in-process connection, but we support it for the sake of testing and
        # making direct access a drop in replacement for the normal service.
        cache=cache,
        offline=offline,
        special_clients=special_clients,
        token_cache=token_cache,
    )


def from_client(
    client,
    structure_clients="numpy",
    *,
    username=None,
    cache=None,
    offline=False,
    path=None,
    special_clients=None,
    token_cache=DEFAULT_TOKEN_CACHE,
):
    """
    Advanced: Connect to a Catalog using a custom instance of httpx.Client or httpx.AsyncClient.

    Parameters
    ----------
    client : httpx.Client
        Should be pre-configured with a base_url and any auth-related headers.
    structure_clients : str or dict, optional
        Use "dask" for delayed data loading and "numpy" for immediate
        in-memory structures (e.g. normal numpy arrays, pandas
        DataFrames). For advanced use, provide dict mapping
        structure_family names ("array", "dataframe", "variable",
        "data_array", "dataset") to client objects. See
        ``Catalog.DEFAULT_STRUCTURE_CLIENT_DISPATCH``.
    username : str, optional
        Username for authenticated access.
    cache : Cache, optional
    offline : bool, optional
        False by default. If True, rely on cache only.
    special_clients : dict, optional
        Advanced: Map client_type_hint from the server to special client
        catalog objects. See also
        ``Catalog.discover_special_clients()`` and
        ``Catalog.DEFAULT_SPECIAL_CLIENT_DISPATCH``.
    token_cache : str, optional
        Path to directory for storing refresh tokens.
    """
    # Interpret structure_clients="numpy" and structure_clients="dask" shortcuts.
    if isinstance(structure_clients, str):
        structure_clients = Catalog.DEFAULT_STRUCTURE_CLIENT_DISPATCH[structure_clients]
    path = path or []
    # Do entrypoint discovery if it hasn't yet been done.
    if Catalog.DEFAULT_SPECIAL_CLIENT_DISPATCH is None:
        Catalog.discover_special_clients()
    special_clients = collections.ChainMap(
        special_clients or {},
        Catalog.DEFAULT_SPECIAL_CLIENT_DISPATCH,
    )
    if username is not None:
        reauthenticate_client(client, username, token_cache=token_cache)
    instance = Catalog(
        client,
        item=NEEDS_INITIALIZATION,
        username=username,
        offline=offline,
        path=path,
        metadata=NEEDS_INITIALIZATION,
        structure_clients=structure_clients,
        cache=cache,
        special_clients=special_clients,
        root_client_type=Catalog,
        token_cache=token_cache,
    )

    item = instance.item
    metadata = item["attributes"]["metadata"]
    return instance.client_for_item(
        item, path=path, metadata=metadata, sorting=item["attributes"].get("sorting")
    )


def from_profile(name, structure_clients=None, **kwargs):
    """
    Build a Catalog based a 'profile' (a named configuration).

    List available profiles and the source filepaths from Python like:

    >>> from tiled.client.profiles import list_profiles
    >>> list_profiles()

    or from a CLI like:

    $ tiled profile list

    Or show the file contents like:

    >>> from tiled.client.profiles import load_profiles
    >>> load_profiles()

    or from a CLI like:

    $ tiled profile show PROFILE_NAME

    Any additional parameters override profile content. See from_uri for details.
    """
    from ..profiles import load_profiles, paths, ProfileNotFound

    profiles = load_profiles()
    try:
        filepath, profile_content = profiles[name]
    except KeyError as err:
        raise ProfileNotFound(
            f"Profile {name!r} not found. Found profiles {list(profiles)} "
            f"from directories {paths}."
        ) from err
    merged = {**profile_content, **kwargs}
    if structure_clients is not None:
        merged["structure_clients"] = structure_clients
    cache_config = merged.pop("cache", None)
    if cache_config is not None:
        from tiled.client.cache import Cache

        if isinstance(cache_config, collections.abc.Mapping):
            # All necessary validation has already been performed
            # in load_profiles().
            ((key, value),) = cache_config.items()
            if key == "memory":
                cache = Cache.in_memory(**value)
            elif key == "disk":
                cache = Cache.on_disk(**value)
        else:
            # Interpret this as a Cache object passed in directly.
            cache = cache_config
        merged["cache"] = cache
    structure_clients_ = merged.pop("structure_clients", None)
    if structure_clients_ is not None:
        if isinstance(structure_clients_, str):
            # Nothing to do.
            merged["structure_clients"] = structure_clients_
        else:
            # This is a dict mapping structure families like "array" and "dataframe"
            # to values. The values may be client objects or importable strings.
            result = {}
            for key, value in structure_clients_.items():
                if isinstance(value, str):
                    class_ = import_object(value)
                else:
                    class_ = value
                result[key] = class_
            merged["structure_clients"] = result
    if "direct" in merged:
        # The profiles specifies that there is no server. We should create
        # an app ourselves and use it directly via ASGI.
        from ..config import construct_serve_catalog_kwargs

        serve_catalog_kwargs = construct_serve_catalog_kwargs(
            merged.pop("direct", None), source_filepath=filepath
        )
        return from_catalog(**serve_catalog_kwargs, **merged)
    else:
        return from_uri(**merged)


def from_config(config):
    """
    Build Catalogs directly, running the app in this same process.

    NOTE: This is experimental. It may need to be re-designed or even removed.

    Parameters
    ----------
    config : str or dict
        May be:

        * Path to config file
        * Path to directory of config files
        * Dict of config

    Examples
    --------

    From config file:

    >>> from_config("path/to/file.yml")

    From directory of config files:

    >>> from_config("path/to/directory")

    From configuration given directly, as dict:

    >>> from_config(
            {
                "catalogs":
                    [
                        "path": "/",
                        "catalog": "tiled.files.Catalog.from_files",
                        "args": {"diretory": "path/to/files"}
                    ]
            }
        )
    """

    from ..config import direct_access

    catalog = direct_access(config)
    return from_catalog(catalog)


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
