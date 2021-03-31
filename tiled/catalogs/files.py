import collections
import functools
import importlib
import mimetypes
import os
from pathlib import Path
import threading
import time

from watchgod.watcher import AllWatcher, Change

from ..structures.dataframe import XLSX_MIME_TYPE
from ..utils import CachingMap, DictView, OneShotCachedMap
from .in_memory import Catalog as CatalogInMemory


class Watcher(AllWatcher):
    def should_watch_file(self, entry) -> bool:
        # TODO Implement ignoring some files.
        return super().should_watch_file(entry)


class Catalog(CatalogInMemory):
    """
    Make a Catalog from files.
    """

    __slots__ = (
        "_readers_by_mimetype",
        "_watcher_thread_kill_switch",
        "_index",
    )

    # This maps MIME types (i.e. file formats) for appropriate Readers.
    # OneShotCachedMap is used to defer imports. We don't want to pay up front
    # for importing Readers that we will not actually use.
    DEFAULT_READERS_BY_MIMETYPE = OneShotCachedMap(
        {
            "image/tiff": lambda: importlib.import_module(
                "...readers.tiff", Catalog.__module__
            ).TiffReader,
            "text/csv": lambda: importlib.import_module(
                "...readers.dataframe", Catalog.__module__
            ).DataFrameAdapter.read_csv,
            XLSX_MIME_TYPE: lambda: importlib.import_module(
                "...readers.excel", Catalog.__module__
            ).ExcelReader.from_file,
        }
    )

    @classmethod
    def from_directory(
        cls,
        directory,
        ignore=None,
        readers_by_mimetype=None,
        metadata=None,
        access_policy=None,
        authenticated_identity=None,
    ):
        if ignore is not None:
            # TODO Support regex or glob (?) patterns to ignore.
            raise NotImplementedError
        # User-provided readers take precedence over defaults.
        readers_by_mimetype = collections.ChainMap(
            readers_by_mimetype or {}, cls.DEFAULT_READERS_BY_MIMETYPE
        )
        # Map subdirectory path parts, as in ('a', 'b', 'c'), to mapping of partials.
        # This single index represents the entire nested directory structure. (We
        # could have done this recursively, with each sub-Catalog watching its own
        # subdirectory, but there are efficiencies to be gained by doing a single
        # walk of the nested directory structure and having a single thread watching
        # for changes within that structure.)
        index = {(): {}}
        # 1. Start watching directory for changes and accumulating a queue of them.
        # 2. Do an initial scan of the files in the directory.
        # 3. When the initial scan completes, start processing changes. This
        #    will cover changes that occurred during or after the initial scan and
        #    avoid a possibile a race condition.
        initial_scan_complete = []
        watcher_thread_kill_switch = []
        watcher_thread = threading.Thread(
            target=_watch,
            args=(
                directory,
                index,
                readers_by_mimetype,
                initial_scan_complete,
                watcher_thread_kill_switch,
            ),
            daemon=True,
            name="tiled-watch-filesystem-changes",
        )
        watcher_thread.start()
        for root, subdirectories, files in os.walk(directory, topdown=True):
            parts = Path(root).relative_to(directory).parts
            for subdirectory in subdirectories:
                # Make a new mapping and a corresponding Catalog for this subdirectory.
                mapping = {}
                index[parts + (subdirectory,)] = mapping
                index[parts][subdirectory] = functools.partial(
                    CatalogInMemory, CachingMap(mapping)
                )
            for filename in files:
                # Add items to the mapping for this root directory.
                index[parts][filename] = _reader_factory_for_file(
                    readers_by_mimetype,
                    Path(root, filename),
                )
        # Appending any object will cause bool(initial_scan_complete) to
        # evaluate to True.
        initial_scan_complete.append(object())
        mapping = CachingMap(index[()])
        return cls(
            mapping,
            index=index,
            readers_by_mimetype=readers_by_mimetype,
            watcher_thread_kill_switch=watcher_thread_kill_switch,
            metadata=metadata,
            authenticated_identity=authenticated_identity,
            access_policy=access_policy,
        )

    def __init__(
        self,
        mapping,
        index,
        readers_by_mimetype,
        watcher_thread_kill_switch,
        metadata,
        access_policy,
        authenticated_identity,
    ):
        super().__init__(
            mapping,
            metadata=metadata,
            access_policy=access_policy,
            authenticated_identity=authenticated_identity,
        )
        self._readers_by_mimetype = readers_by_mimetype
        self._watcher_thread_kill_switch = watcher_thread_kill_switch
        self._index = index

    @property
    def readers_by_mimetype(self):
        return DictView(self._readers_by_mimetype)

    def new_variation(self, *args, **kwargs):
        return super().new_variation(
            *args,
            watcher_thread_kill_switch=self._watcher_thread_kill_switch,
            readers_by_mimetype=self._readers_by_mimetype,
            index=self._index,
            **kwargs,
        )

    def shutdown_watcher(self):
        # Appending any object will cause bool(self._watcher_thread_kill_switch)
        # to evaluate to True.
        self._watcher_thread_kill_switch.append(object())


def _watch(
    directory,
    index,
    readers_by_mimetype,
    initial_scan_complete,
    watcher_thread_kill_switch,
    poll_interval=0.2,
):
    watcher = Watcher(directory)
    queued_changes = []
    while not watcher_thread_kill_switch:
        changes = watcher.check()
        if initial_scan_complete:
            # Process initial backlog. (This only happens once, ever.)
            if queued_changes:
                _process_changes(queued_changes, directory, readers_by_mimetype, index)
            # Process changes just collected.
            _process_changes(changes, directory, readers_by_mimetype, index)
        else:
            # The initial scan is still going. Stash the changes for later.
            queued_changes.extend(changes)
        time.sleep(poll_interval)


def _process_changes(changes, directory, readers_by_mimetype, index):
    for kind, entry in changes:
        path = Path(entry)
        if path.is_dir():
            raise NotImplementedError
        parent_parts = path.relative_to(directory).parent.parts
        if kind == Change.added:
            index[parent_parts][path.name] = _reader_factory_for_file(
                readers_by_mimetype,
                path,
            )
        elif kind == Change.deleted:
            index[parent_parts].pop(path.name)
        elif kind == Change.modified:
            # Nothing to do at present, but once we add caching we'll need to
            # invalidate the relevant cache entry here.
            pass


def _reader_factory_for_file(readers_by_mimetype, path):
    mimetype, _ = mimetypes.guess_type(path)
    reader_class = readers_by_mimetype[mimetype]
    return functools.partial(reader_class, str(path))
