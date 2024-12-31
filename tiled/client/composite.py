from .container import Container

import collections
import collections.abc
import functools
import importlib
import itertools
import time
import warnings
from dataclasses import asdict
from urllib.parse import parse_qs, urlparse
import copy

import entrypoints
import httpx

from ..adapters.utils import IndexersMixin
from ..iterviews import ItemsView, KeysView, ValuesView
from ..queries import KeyLookup
from ..query_registration import query_registry
from ..structures.core import Spec, StructureFamily
from ..structures.table import TableStructure
from ..structures.data_source import DataSource
from ..utils import UNCHANGED, OneShotCachedMap, Sentinel, node_repr, safe_json_dump
from .base import STRUCTURE_TYPES, BaseClient
from .utils import (
    MSGPACK_MIME_TYPE,
    ClientError,
    client_for_item,
    export_util,
    handle_error,
    normalize_specs
)
from .container import DEFAULT_STRUCTURE_CLIENT_DISPATCH, _write_partition

LENGTH_CACHE_TTL = 1  # second


class CompositeClient(BaseClient, collections.abc.Mapping, IndexersMixin):
    # This maps the structure_family sent by the server to a client-side object that
    # can interpret the structure_family's structure and content. OneShotCachedMap is used to
    # defer imports.

    # This is populated when the first instance is created.
    STRUCTURE_CLIENTS_FROM_ENTRYPOINTS = None

    @classmethod
    def discover_clients_from_entrypoints(cls):
        """
        Search the software environment for libraries that register structure clients.

        This is called once automatically the first time Node.from_uri
        is called. It is idempotent.
        """
        if cls.STRUCTURE_CLIENTS_FROM_ENTRYPOINTS is not None:
            # short-circuit
            return
        # The modules associated with these entrypoints will be imported
        # lazily, only when the item is first accessed.
        cls.STRUCTURE_CLIENTS_FROM_ENTRYPOINTS = OneShotCachedMap()
        # Check old name (special_client) and new name (structure_client).
        for entrypoint_name in ["tiled.special_client", "tiled.structure_client"]:
            for name, entrypoint in entrypoints.get_group_named(
                entrypoint_name
            ).items():
                cls.STRUCTURE_CLIENTS_FROM_ENTRYPOINTS.set(name, entrypoint.load)
                DEFAULT_STRUCTURE_CLIENT_DISPATCH["numpy"].set(name, entrypoint.load)
                DEFAULT_STRUCTURE_CLIENT_DISPATCH["dask"].set(name, entrypoint.load)

    def __repr__(self):
        # Display up to the first N flat_keys from the inlined structure.
        N = 10
        return node_repr(self, self._keys_slice(0, N, direction=1))

    @property
    def sorting(self):
        """
        The current sorting of this Node

        Given as a list of tuples where the first entry is the sorting key
        and the second entry indicates ASCENDING (or 1) or DESCENDING (or -1).
        """
        return list(self._sorting)
    
    @property
    def parts(self):
        structure = self.structure()
        if structure and structure.contents:
            return CompositeContents(self)

    def __len__(self) -> int:
        # If the contents of this node was provided in-line, there is an
        # implication that the contents are not expected to be dynamic. Used the
        # count provided in the structure.
        if self.structure() and (self.structure().count is not None):
            return self.structure().count
        return 0

    def __length_hint__(self):
        # TODO The server should provide an estimated count.
        # https://www.python.org/dev/peps/pep-0424/
        return len(self)

    def __iter__(self):
        # If the contents of this node was provided in-line, and we don't need
        # to apply any filtering or sorting, we can slice the in-lined data
        # without fetching anything from the server.
        structure = self.structure()
        if structure and structure.contents:
            return (yield from structure.contents)

    def __getitem__(self, key):
        if key not in self.structure().flat_keys:
            # Only allow getting from flat_keys, not parts
            raise KeyError(key)
        try:
            self_link = self.item["links"]["self"].rstrip('/')
            url_path = f"{self_link}/{key}"
            params = parse_qs(urlparse(url_path).query)
            if self._include_data_sources:
                params["include_data_sources"] = True
            content = handle_error(
                self.context.http_client.get(
                    url_path,
                    headers={"Accept": MSGPACK_MIME_TYPE},
                    params=params,
                )
            ).json()
        except ClientError as err:
            if err.response.status_code == 404:
                raise KeyError(key)
            raise
        item = content["data"]
        return client_for_item(
            self.context,
            self.structure_clients,
            item,
            include_data_sources=self._include_data_sources,
        )

    def delete(self, key):
        handle_error(self.context.http_client.delete(f"{self.uri}/{key}"))

    # The following two methods are used by keys(), values(), items().

    def _keys_slice(self, start, stop, direction):
        # If the contents of this node was provided in-line (default),
        # we can slice the in-lined data without fetching anything from the server.
        self.refresh()
        contents = self.item["attributes"]["structure"]["contents"]
        if contents is not None:
            keys = []
            for key, item in contents.items():
                if item["attributes"]['structure_family'] == StructureFamily.table:
                    keys.extend(item["attributes"]['structure']['columns'])
                else:
                    keys.append(key)
            if direction < 0:
                keys = list(reversed(keys))
            return (yield from keys[start:stop])

    def _items_slice(self, start, stop, direction):
        # If the contents of this node was provided in-line (default),
        # we can slice the in-lined data without fetching anything from the server.
        self.refresh()
        contents = self.item["attributes"]["structure"]["contents"]
        if contents is not None:
            lazy_items = []
            for key, item in contents.items():
                if item["attributes"]['structure_family'] == StructureFamily.table:
                    for col in item["attributes"]['structure']['columns']:
                        lazy_items.append((col, lambda : self[col]))
                else:
                    lazy_items.append(( key, lambda : client_for_item(self.context, self.structure_clients, item, include_data_sources=self._include_data_sources) ))

            if direction < 0:
                lazy_items = list(reversed(lazy_items))
            for key, lazy_item in lazy_items[start:stop]:
                yield key, lazy_item()
            return

    def keys(self):
        return KeysView(lambda: len(self), self._keys_slice)

    def values(self):
        return ValuesView(lambda: len(self), self._items_slice)

    def items(self):
        return ItemsView(lambda: len(self), self._items_slice)

    def export(self, filepath, fields=None, *, format=None):
        """
        Download metadata and data below this node in some format and write to a file.

        Parameters
        ----------
        file: str or buffer
            Filepath or writeable buffer.
        fields: List[str], optional
            Filter which items in this node to export.
        format : str, optional
            If format is None and `file` is a filepath, the format is inferred
            from the name, like 'table.h5' implies format="application/x-hdf5". The format
            may be given as a file extension ("h5") or a media type ("application/x-hdf5").

        Examples
        --------

        Export all.

        >>> a.export("everything.h5")

        """
        params = {}
        if fields is not None:
            params["field"] = fields
        return export_util(
            filepath,
            format,
            self.context.http_client.get,
            self.item["links"]["full"],
            params=params,
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


    #TODO: The following methods can be subclassed from client.Container

    # When (re)chunking arrays for upload, we use this limit
    # to attempt to avoid bumping into size limits.
    _SUGGESTED_MAX_UPLOAD_SIZE = 100_000_000  # 100 MB

    def new(
        self,
        structure_family,
        data_sources,
        *,
        key=None,
        metadata=None,
        specs=None,
    ):
        return Container.new(self, structure_family, data_sources, key=key, metadata=metadata, specs=specs)

    def write_array(self, array, *, key=None, metadata=None, dims=None, specs=None):
        return Container.write_array(self, array, key=key, metadata=metadata, dims=dims, specs=specs)

    def write_awkward(
        self,
        array,
        *,
        key=None,
        metadata=None,
        dims=None,
        specs=None,
    ):
        return Container.write_awkward(self, array, key=key, metadata=metadata, dims=dims, specs=specs)

    def write_sparse(
        self,
        coords,
        data,
        shape,
        *,
        key=None,
        metadata=None,
        dims=None,
        specs=None,
    ):
        return Container.write_sparse(self, coords, data, shape, key=key, metadata=metadata, dims=dims, specs=specs)

    def write_dataframe(
        self,
        dataframe,
        *,
        key=None,
        metadata=None,
        specs=None,
    ):
        return Container.write_dataframe(self, dataframe, key=key, metadata=metadata, specs=specs)


class CompositeContents:
    def __init__(self, node):
        self.contents = node.structure().contents
        self.links = node.item['links']
        self.context = node.context
        self.structure_clients = node.structure_clients
        self._include_data_sources = node._include_data_sources

    def __repr__(self):
        return (
            f"<{type(self).__name__} {{"
            + ", ".join(f"'{item}'" for item in self.contents)
            + "}>"
        )

    def __getitem__(self, key):
        if key not in self.contents:
            raise KeyError(key)
        try:
            item = self.contents[key]
            url_path = self.links['self']
            params = parse_qs(urlparse(url_path).query)
            params["part"] = key
            if self._include_data_sources:
                params["include_data_sources"] = True
            content = handle_error(
                self.context.http_client.get(
                    url_path,
                    headers={"Accept": MSGPACK_MIME_TYPE},
                    params=params,
                )
            ).json()
        except ClientError as err:
            if err.response.status_code == 404:
                raise KeyError(key)
            raise
        item = content["data"]
        return client_for_item(
            self.context,
            self.structure_clients,
            item,
            include_data_sources=self._include_data_sources,
        )

    def __iter__(self):
        for key in self.contents:
            yield key
