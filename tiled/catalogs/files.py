import collections
import functools
import importlib
import mimetypes
import os
from pathlib import Path
import re
import threading
import time
import warnings

from watchgod.watcher import RegExpWatcher, Change

from ..structures.dataframe import XLSX_MIME_TYPE
from ..utils import CachingMap, import_object, OneShotCachedMap
from .in_memory import Catalog as CatalogInMemory


class Catalog(CatalogInMemory):
    """
    A Catalog constructed by walking a directory and watching it for changes.

    Examples
    --------

    >>> Catalog.from_directory("path/to/directory")
    """

    __slots__ = (
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
            "application/x-hdf5": lambda: importlib.import_module(
                "...readers.hdf5", Catalog.__module__
            ).HDF5Reader.from_file,
        }
    )

    @classmethod
    def from_directory(
        cls,
        directory,
        ignore_re_dirs=None,
        ignore_re_files=None,
        readers_by_mimetype=None,
        mimetypes_by_file_ext=None,
        metadata=None,
        access_policy=None,
        authenticated_identity=None,
        error_if_missing=True,
    ):
        if error_if_missing:
            if not os.path.isdir(directory):
                raise ValueError(
                    f"{directory} is not a directory. "
                    "To run anyway, in anticipation of the directory "
                    "appearing later, use error_if_missing=False."
                )
        readers_by_mimetype = readers_by_mimetype or {}
        # If readers_by_mimetype comes from a configuration file,
        # objects are given as importable strings, like "package.module:Reader".
        for key, value in list(readers_by_mimetype.items()):
            if isinstance(value, str):
                readers_by_mimetype[key] = import_object(value)
        # User-provided readers take precedence over defaults.
        merged_readers_by_mimetype = collections.ChainMap(
            readers_by_mimetype, cls.DEFAULT_READERS_BY_MIMETYPE
        )
        mimetypes_by_file_ext = mimetypes_by_file_ext or {}
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
                ignore_re_files,
                ignore_re_dirs,
                index,
                merged_readers_by_mimetype,
                mimetypes_by_file_ext,
                initial_scan_complete,
                watcher_thread_kill_switch,
            ),
            daemon=True,
            name="tiled-watch-filesystem-changes",
        )
        watcher_thread.start()
        compiled_ignore_re_dirs = (
            re.compile(ignore_re_dirs) if ignore_re_dirs is not None else ignore_re_dirs
        )
        compiled_ignore_re_files = (
            re.compile(ignore_re_files)
            if ignore_re_files is not None
            else ignore_re_files
        )
        for root, subdirectories, files in os.walk(directory, topdown=True):
            parts = Path(root).relative_to(directory).parts
            # Account for ignore_re_dirs and update which subdirectories we will traverse.
            valid_subdirectories = []
            for d in subdirectories:
                if (ignore_re_dirs is None) or compiled_ignore_re_dirs.match(
                    str(Path(*(parts + (d,))))
                ):
                    valid_subdirectories.append(d)
            subdirectories[:] = valid_subdirectories
            for subdirectory in subdirectories:
                # Make a new mapping and a corresponding Catalog for this subdirectory.
                mapping = {}
                index[parts + (subdirectory,)] = mapping
                index[parts][subdirectory] = functools.partial(
                    CatalogInMemory, CachingMap(mapping)
                )
            # Account for ignore_re_files and update which files we will traverse.
            valid_files = []
            for f in files:
                if (ignore_re_files is None) or compiled_ignore_re_files.match(
                    str(Path(*(parts + (f,))))
                ):
                    valid_files.append(f)
            files[:] = valid_files
            for filename in files:
                if (ignore_re_files is not None) and compiled_ignore_re_files.match(
                    str(Path(*parts))
                ):
                    continue
                # Add items to the mapping for this root directory.
                try:
                    index[parts][filename] = _reader_factory_for_file(
                        merged_readers_by_mimetype,
                        mimetypes_by_file_ext,
                        Path(root, filename),
                    )
                except NoReaderAvailable:
                    pass
        # Appending any object will cause bool(initial_scan_complete) to
        # evaluate to True.
        initial_scan_complete.append(object())
        mapping = CachingMap(index[()])
        return cls(
            mapping,
            index=index,
            watcher_thread_kill_switch=watcher_thread_kill_switch,
            metadata=metadata,
            authenticated_identity=authenticated_identity,
            access_policy=access_policy,
        )

    def __init__(
        self,
        mapping,
        index,
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
        self._watcher_thread_kill_switch = watcher_thread_kill_switch
        self._index = index

    def new_variation(self, *args, **kwargs):
        return super().new_variation(
            *args,
            watcher_thread_kill_switch=self._watcher_thread_kill_switch,
            index=self._index,
            **kwargs,
        )

    def shutdown_watcher(self):
        # Appending any object will cause bool(self._watcher_thread_kill_switch)
        # to evaluate to True.
        self._watcher_thread_kill_switch.append(object())


def _watch(
    directory,
    ignore_re_files,
    ignore_re_dirs,
    index,
    readers_by_mimetype,
    mimetypes_by_file_ext,
    initial_scan_complete,
    watcher_thread_kill_switch,
    poll_interval=0.2,
):
    watcher = RegExpWatcher(
        directory,
        re_files=ignore_re_files,
        re_dirs=ignore_re_dirs,
    )
    queued_changes = []
    while not watcher_thread_kill_switch:
        changes = watcher.check()
        if initial_scan_complete:
            # Process initial backlog. (This only happens once, ever.)
            if queued_changes:
                _process_changes(
                    queued_changes,
                    directory,
                    readers_by_mimetype,
                    mimetypes_by_file_ext,
                    index,
                )
            # Process changes just collected.
            _process_changes(
                changes, directory, readers_by_mimetype, mimetypes_by_file_ext, index
            )
        else:
            # The initial scan is still going. Stash the changes for later.
            queued_changes.extend(changes)
        time.sleep(poll_interval)


def _process_changes(
    changes, directory, readers_by_mimetype, mimetypes_by_file_ext, index
):
    ignore = set()
    for kind, entry in changes:
        path = Path(entry)
        if path in ignore:
            # We have seen this before and could not find a Reader for it.
            # Do not try again.
            continue
        if path.is_dir():
            raise NotImplementedError
        parent_parts = path.relative_to(directory).parent.parts
        if kind == Change.added:
            try:
                index[parent_parts][path.name] = _reader_factory_for_file(
                    readers_by_mimetype,
                    mimetypes_by_file_ext,
                    path,
                )
            except NoReaderAvailable:
                # Ignore this file in the future.
                # We already know that we do not know how to find a Reader
                # for this filename.
                ignore.add(path)
        elif kind == Change.deleted:
            index[parent_parts].pop(path.name)
        elif kind == Change.modified:
            # Why do we need a try/except here? A reasonable question!
            # Normally, we would learn about the file first via a Change.added
            # or via the initial scan. Then, later, when we learn about modification
            # we can be sure that we already know how to find a Reader for this
            # filename. But, during that initial scan, there is a race condition
            # where we might learn about Change.modified before we first add that file
            # to our index. Therefore, we guard this with a try/except, knowing
            # that this could be the first time we see this path.
            try:
                index[parent_parts][path.name] = _reader_factory_for_file(
                    readers_by_mimetype,
                    mimetypes_by_file_ext,
                    path,
                )
            except NoReaderAvailable:
                # Ignore this file in the future.
                # We already know that we do not know how to find a Reader
                # for this filename.
                ignore.add(path)


def _reader_factory_for_file(readers_by_mimetype, mimetypes_by_file_ext, path):
    ext = "".join(path.suffixes)  # e.g. ".h5" or ".tar.gz"
    if ext in mimetypes_by_file_ext:
        mimetype = mimetypes_by_file_ext[ext]
    else:
        # Use the Python's built-in facility for guessing mimetype
        # from file extension. This loads data about mimetypes from
        # the operating system the first time it is used.
        mimetype, _ = mimetypes.guess_type(path)
    if mimetype is None:
        msg = (
            f"The file at {path} has a file extension {ext} this is not "
            "recognized. The file will be skipped, pass in a mimetype "
            "for this file extension via the parameter "
            "Catalog.from_directory(..., mimetypes_by_file_ext={...}) and "
            "pass in a Reader than handles this mimetype via "
            "the parameter Catalog.from_directory(..., readers_by_mimetype={...})."
        )
        warnings.warn(msg)
        raise NoReaderAvailable
    try:
        reader_class = readers_by_mimetype[mimetype]
    except KeyError:
        msg = (
            f"The file at {path} was recognized as mimetype {mimetype} "
            "but there is no reader for that mimetype. The file will be skipped. "
            "To fix this, pass in a Reader than handles this mimetype via "
            "the parameter Catalog.from_directory(..., readers_by_mimetype={...})."
        )
        warnings.warn(msg)
        raise NoReaderAvailable
    return functools.partial(reader_class, str(path))


class NoReaderAvailable(Exception):
    pass
