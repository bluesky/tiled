import collections
import collections.abc
import functools
import importlib
import mimetypes
import os
from pathlib import Path
import threading
import time

from watchgod.watcher import AllWatcher, Change

from .in_memory import Catalog as CatalogInMemory
from ..utils import LazyMap


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


class Watcher(AllWatcher):
    def should_watch_file(self, entry) -> bool:
        # TODO Implement ignoring some files.
        return super().should_watch_file(entry)


class Catalog(CatalogInMemory):
    """
    Make a Catalog from files.
    """

    # This maps MIME types (i.e. file formats) for appropriate Readers.
    # LazyMap is used to defer imports. We don't want to pay up front
    # for importing Readers that we will not actually use.
    DEFAULT_READERS_BY_MIMETYPE = LazyMap(
        {
            "image/tiff": lambda: importlib.import_module(
                "...readers.tiff", Catalog.__module__
            ).TiffReader,
        }
    )

    def __init__(self, directory, ignore=None, readers_by_mimetype=None):
        self.directory = directory
        if ignore is not None:
            # TODO Support regex or glob (?) patterns to ignore.
            raise NotImplementedError
        # User-provided readers take precedence over defaults.
        self.readers_by_mimetype = collections.ChainMap(
            readers_by_mimetype or {}, self.DEFAULT_READERS_BY_MIMETYPE
        )
        # 1. Start watching directory for changes and accumulating a queue of them.
        # 2. Do an initial scan of the files in the directory.
        # 3. When the initial scan completes, start processing changes. This
        #    will cover changes that occurred during or after the initial scan.
        self.watcher = Watcher(directory)
        self._initial_scan_complete = False
        self._queued_changes = []
        self._watcher_thread_kill_switch = False
        self._watching_thread = threading.Thread(
            target=self._watch,
            daemon=True,
            name="tiled-watch-filesystem-changes",
        )
        self._watching_thread.start()
        # Map subdirectory path parts, as in ('a', 'b', 'c'), to mapping of partials.
        # This single index represents the entire nested directory structure. (We
        # could have done this recursively, with each sub-Catalog watching its own
        # subdirectory, but there are efficiencies to be gained by doing a single
        # walk of the nested directory structure and having a single thread watching
        # for changes within that structure.)
        index = {(): {}}
        for root, subdirectories, files in os.walk(directory, topdown=True):
            parts = Path(root).relative_to(directory).parts
            for subdirectory in subdirectories:
                # Make a new mapping and a corresponding Catalog for this subdirectory.
                mapping = {}
                index[parts + (subdirectory,)] = mapping
                index[parts][subdirectory] = functools.partial(
                    CatalogInMemory, LazyCachedMap(mapping)
                )
            for filename in files:
                # Add items to the mapping for this root directory.
                index[parts][filename] = self._reader_factory_for_file(
                    Path(root, filename)
                )
        self._index = index
        self._initial_scan_complete = True
        super().__init__(LazyCachedMap(self._index[()]))

    def _reader_factory_for_file(self, path):
        mimetype, _ = mimetypes.guess_type(path)
        reader_class = self.readers_by_mimetype[mimetype]
        return functools.partial(reader_class, str(path))

    def shutdown_watcher(self):
        self._watcher_thread_kill_switch = True

    def _watch(self, poll_interval=0.2):
        while not self._watcher_thread_kill_switch:
            changes = self.watcher.check()
            if self._initial_scan_complete:
                # Process initial backlog. (This only happens once, ever.)
                if self._queued_changes:
                    self._process_changes(self._queued_changes)
                # Process changes just collected.
                self._process_changes(changes)
            else:
                # The initial scan is still going. Stash the changes for later.
                self._queued_changes.extend(changes)
            time.sleep(poll_interval)

    def _process_changes(self, changes):
        for kind, entry in changes:
            path = Path(entry)
            if path.is_dir():
                raise NotImplementedError
            parent_parts = path.relative_to(self.directory).parent.parts
            if kind == Change.added:
                self._index[parent_parts][path.name] = self._reader_factory_for_file(
                    path
                )
            elif kind == Change.deleted:
                self._index[parent_parts].pop(path.name)
            elif kind == Change.modified:
                # Nothing to do at present, but once we add caching we'll need to
                # invalidate the relevant cache entry here.
                pass
