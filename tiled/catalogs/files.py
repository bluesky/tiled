import collections
import collections.abc
import functools
import mimetypes
import os
from pathlib import Path
import threading

import watchgod

from .in_memory import Catalog as CatalogInMemory
from ..readers.array import (
    ArrayReader,
)  # TODO If this is needed, its import should be delayed.


class TiffReader(ArrayReader):
    def __init__(self, path):
        import tifffile

        super().__init__(tifffile.imread(path))


class LazyCachedMap(collections.abc.Mapping):
    """
    Mapping that computes values on read and optionally caches them.

    Parameters
    ----------
    mapping : dict-like
        Must map keys to callables that return values.
    cache : dict-like or None, optional
        Will be used to cache values. If None, nothing is cached.
        May be ordinary dict, LRUCache, etc.
    """

    __slots__ = ("__mapping", "__cache")

    def __init__(self, mapping, cache=None):
        self.__mapping = mapping
        self.__cache = cache

    def __getitem__(self, key):
        if self.__cache is None:
            return self.__mapping[key]()
        else:
            try:
                return self.__cache[key]
            except KeyError:
                value = self.__mapping[key]()
                self.__cache[key] = value
                return value

    def __len__(self):
        return len(self.__mapping)

    def __iter__(self):
        return iter(self.__mapping)

    def __contains__(self, key):
        # Ensure checking 'in' does not trigger evaluation.
        return key in self.__mapping

    def __getstate__(self):
        return self.__mapping, self.__cache

    def __setstate__(self, mapping, cache):
        self.__mapping = mapping
        self.__cache = cache

    def __repr__(self):
        if self.__cache is None:
            d = {k: "<lazy>" for k in self.__mapping}
        else:
            d = {}
            for k in self.__mapping:
                try:
                    value = self.__cache[k]
                except KeyError:
                    d[k] = "<lazy>"
                else:
                    d[k] = repr(value)
        return (
            f"<{type(self).__name__}"
            "({" + ", ".join(f"{k!r}: {v!s}" for k, v in d.items()) + "})>"
        )


class Catalog(CatalogInMemory):
    """
    Make a Catalog from files.
    """

    DEFAULT_READERS_BY_MIMETYPE = {
        "image/tiff": TiffReader,
    }

    def __init__(self, directory, ignore=None, readers_by_mimetype=None):
        if ignore is not None:
            # TODO Support regex or glob (?) patterns to ignore.
            raise NotImplementedError
        readers = self.DEFAULT_READERS_BY_MIMETYPE.copy()
        readers.update(readers_by_mimetype or {})
        self.readers_by_mimetype = readers
        # 1. Start watching directory for changes and accumulating a queue of them.
        # 2. Do an initial scan of the files in the directory.
        # 3. When the initial scan completes, start processing changes. This
        #    will cover changes that occurred during or after the initial scan.
        self._watching_thread = threading.Thread(
            target=self._watch, args=(directory,), name="tiled-watch-filesystem-changes"
        )
        self._watching_thread.start()
        internal_mapping = {}
        catalog_mapping = {}
        for root, subdirectories, files in os.walk(directory, topdown=True):
            internal_mapping_node = internal_mapping
            catalog_mapping_node = catalog_mapping
            for part in Path(root).relative_to(directory).parts:
                internal_mapping_node = internal_mapping_node[part]
                catalog_mapping_node = catalog_mapping_node[part]
            for subdirectory in subdirectories:
                internal_mapping_node[subdirectory] = {}
                catalog_mapping_node[subdirectory] = functools.partial(
                    CatalogInMemory, LazyCachedMap(internal_mapping_node[subdirectory])
                )
            for filename in files:
                internal_mapping_node[filename] = self._reader_factory_for_file(
                    Path(root, filename)
                )
        self._internal_mapping = internal_mapping
        self._catalog_mapping = catalog_mapping
        super().__init__(
            LazyCachedMap(
                collections.ChainMap(self._catalog_mapping, self._internal_mapping)
            )
        )

    def _watch(self, directory):
        for changes in watchgod.watch(directory):
            print(changes)
            # TODO Call _process_file.
            # TODO Shut down cleanly.

    def _reader_factory_for_file(self, path):
        mimetype, _ = mimetypes.guess_type(path)
        reader_class = self.readers_by_mimetype[mimetype]
        return functools.partial(reader_class, str(path))
