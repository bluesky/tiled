import collections
import functools
import importlib
import mimetypes
import os
import queue
import re
import threading
import warnings
from pathlib import Path

import cachetools
from watchgod.watcher import Change, RegExpWatcher

from ..serialization.dataframe import XLSX_MIME_TYPE
from ..utils import CachingMap, OneShotCachedMap, import_object
from .mapping import MapAdapter

# The Adapter objects are light because any large data they stash should be
# placed in the global internal cache, not in the Adapter state itself.
# Therefore, we can afford to accumulate many of these.
MAX_ADAPTER_CACHE_SIZE = 10_000

DEFAULT_POLL_INTERVAL = 0.2  # seconds


def strip_suffixes(filename):
    """
    For use with key_from_filename parameter.

    This gives the 'base' as the key.

    >>> strip_suffixes("a.tif")
    "a"

    >>> strip_suffixes("thing.tar.gz")
    "thing"
    """
    path = Path(filename)
    # You would think there would be a method for this, but there is not.
    if len(path.suffixes):
        return str(path)[: -sum([len(s) for s in path.suffixes])]
    else:
        return filename


def identity(filename):
    """
    For use with key_from_filename parameter.

    This give the full filename (with suffixes) as the key.
    """
    return filename


# This maps MIME types (i.e. file formats) for appropriate Readers.
# OneShotCachedMap is used to defer imports. We don't want to pay up front
# for importing Readers that we will not actually use.
DEFAULT_READERS_BY_MIMETYPE = OneShotCachedMap(
    {
        "image/tiff": lambda: importlib.import_module(
            "...adapters.tiff", __name__
        ).TiffAdapter,
        "text/csv": lambda: importlib.import_module(
            "...adapters.dataframe", __name__
        ).DataFrameAdapter.read_csv,
        XLSX_MIME_TYPE: lambda: importlib.import_module(
            "...adapters.excel", __name__
        ).ExcelAdapter.from_file,
        "application/x-hdf5": lambda: importlib.import_module(
            "...adapters.hdf5", __name__
        ).HDF5Adapter.from_file,
    }
)

# We can mostly rely on mimetypes.types_map for the common ones
# ('.csv' -> 'text/csv', etc.) but we supplement here for some
# of the more exotic ones that not all platforms know about.
DEFAULT_MIMETYPES_BY_FILE_EXT = {
    # This is the "official" file extension.
    ".h5": "application/x-hdf5",
    # This is NeXus. We may want to invent a special media type
    # like 'application/x-nexus' for this, but I'll punt that for now.
    # Needs thought about how to encode the various types of NeXus
    # (media type arguments, for example).
    ".nxs": "application/x-hdf5",
    # These are unofficial but common file extensions.
    ".hdf": "application/x-hdf5",
    ".hdf5": "application/x-hdf5",
    # on opensuse csv -> text/x-comma-separated-values
    ".csv": "text/csv",
}


class DirectoryAdapter(MapAdapter):
    """
    An Adapter constructed by walking a directory and watching it for changes.

    Examples
    --------

    >>> DirecotryAdapter.from_directory("path/to/directory")
    """

    __slots__ = (
        "_watcher_thread_kill_switch",
        "_index",
        "_subdirectory_trie",
        "_subdirectory_handler",
        "_directory",
        "_manual_trigger",
    )

    @classmethod
    def from_directory(
        cls,
        directory,
        *,
        ignore_re_dirs=None,
        ignore_re_files=None,
        readers_by_mimetype=None,
        mimetypes_by_file_ext=None,
        mimetype_detection_hook=None,
        subdirectory_handler=None,
        key_from_filename=strip_suffixes,
        metadata=None,
        sorting=None,
        specs=None,
        access_policy=None,
        principal=None,
        error_if_missing=True,
        greedy=False,
        poll_interval=DEFAULT_POLL_INTERVAL,
        entries_stale_after=None,
        metadata_stale_after=None,
        **kwargs,
    ):
        """
        Construct a Adapter from a directory of files.

        Parameters
        ----------
        ignore_re_dirs : str, optional
            Regular expression. Matched directories will be ignored.
        ignore_re_files : str, optional
            Regular expression. Matched files will be ignored.
        readers_by_mimetype : dict, optional
            Map a mimetype to a Reader suitable for that mimetype
        mimetypes_by_file_ext : dict, optional
            Map a file extension (e.g. '.tif') to a mimetype (e.g. 'image/tiff')
        mimetype_detection_hook: callable, optional
            Signature: f(filepath) -> str

            It may return a registered mimetype like 'text/csv' or
            a custom unregistered mimetype 'text/x-specfile'.
        subdirectory_handler : callable, optional
            Given a (relative) filepath to a direj
        key_from_filename : callable[str] -> str,
            Given a filename, return the key for the item that will represent it.
            By default, this strips off the suffixes, so "a.tif" -> "a".
        metadata : dict, optional,
            Metadata for the top-level node of this tree.
        specs : List[str]
        access_policy : AccessPolicy, optional
        principal : str, optional
        error_if_missing : boolean, optional
            If True (default) raise an error if the directory does not exist.
            If False, wait and poll for the directory to be created later.
        greedy : boolean, optional
            If False (default) instantiate nodes in the tree lazily, when first
            accessed. If True, instantiate them greedily when the underlying
            files are first found.
        poll_interval : float or False, optional
            Time in seconds between scans of the directory for removed or
            changed files. If False or 0, do not poll for changes.
            Default value is 0.2 seconds, subject to change without notice.
        entries_stale_after: timedelta
            This server uses this to communite to the client how long
            it should rely on a local cache before checking back for changes.
        metadata_stale_after: timedelta
            This server uses this to communite to the client how long
            it should rely on a local cache before checking back for changes.
        """

        if error_if_missing:
            if not os.path.isdir(directory):
                raise ValueError(
                    f"{directory} is not a directory. "
                    "To run anyway, in anticipation of the directory "
                    "appearing later, use error_if_missing=False."
                )
        readers_by_mimetype = readers_by_mimetype or {}
        if mimetype_detection_hook is not None:
            mimetype_detection_hook = import_object(mimetype_detection_hook)
        # If readers_by_mimetype comes from a configuration file,
        # objects are given as importable strings, like "package.module:Reader".
        for key, value in list(readers_by_mimetype.items()):
            if isinstance(value, str):
                readers_by_mimetype[key] = import_object(value)
        if isinstance(key_from_filename, str):
            key_from_filename = import_object(key_from_filename)
        if isinstance(subdirectory_handler, str):
            subdirectory_handler = import_object(subdirectory_handler)
        # User-provided readers take precedence over defaults.
        merged_readers_by_mimetype = collections.ChainMap(
            readers_by_mimetype, DEFAULT_READERS_BY_MIMETYPE
        )
        mimetypes_by_file_ext = mimetypes_by_file_ext or {}
        merged_mimetypes_by_file_ext = collections.ChainMap(
            mimetypes_by_file_ext, DEFAULT_MIMETYPES_BY_FILE_EXT
        )
        # Map subdirectory path parts, as in ('a', 'b', 'c'), to mapping of partials.
        # This single index represents the entire nested directory structure. (We
        # could have done this recursively, with each sub-Adapter watching its own
        # subdirectory, but there are efficiencies to be gained by doing a single
        # walk of the nested directory structure and having a single thread watching
        # for changes within that structure.)
        mapping = CachingMap(
            {}, cache=cachetools.LRUCache(maxsize=MAX_ADAPTER_CACHE_SIZE)
        )
        index = {(): mapping}
        # Map key to set of filepaths that map to that key.
        collision_tracker = collections.defaultdict(set)
        # This is a trie for efficiently checking of a given subdirectory is
        # claimed by a subdirectory_handler.
        subdirectory_trie = {}
        # 1. Start watching directory for changes and accumulating a queue of them.
        # 2. Do an initial scan of the files in the directory.
        # 3. When the initial scan completes, start processing changes. This
        #    will cover changes that occurred during or after the initial scan and
        #    avoid a possibile a race condition.
        initial_scan_complete = []
        watcher_thread_kill_switch = []
        manual_trigger = queue.Queue()
        watcher_thread = threading.Thread(
            target=_watch,
            args=(
                directory,
                ignore_re_files,
                ignore_re_dirs,
                index,
                subdirectory_trie,
                subdirectory_handler,
                merged_readers_by_mimetype,
                merged_mimetypes_by_file_ext,
                mimetype_detection_hook,
                key_from_filename,
                initial_scan_complete,
                watcher_thread_kill_switch,
                manual_trigger,
                greedy,
                collision_tracker,
                poll_interval,
            ),
            daemon=True,
            name="tiled-watch-filesystem-changes",
        )
        if poll_interval:
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
            # Skip this root if it corresponds to a directory managed by a handler.
            # TODO Let the top-level directory be managed by a handler?
            if parts:
                d = subdirectory_trie
                for part in parts:
                    if part not in d:
                        this_root_is_separately_managed = False
                        break
                    if not isinstance(d[part], dict):
                        this_root_is_separately_managed = True
                        break
                    d = d[part]
                else:
                    this_root_is_separately_managed = True
                if this_root_is_separately_managed:
                    continue

            # Account for ignore_re_dirs and update which subdirectories we will traverse.
            valid_subdirectories = []
            for d in subdirectories:
                if (ignore_re_dirs is None) or compiled_ignore_re_dirs.match(
                    str(Path(*(parts + (d,))))
                ):
                    valid_subdirectories.append(d)
            subdirectories[:] = valid_subdirectories
            for subdirectory in subdirectories:
                _new_subdir(
                    index,
                    subdirectory_trie,
                    subdirectory_handler,
                    root,
                    parts,
                    subdirectory,
                    greedy,
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
                key = key_from_filename(filename)
                filepath = Path(*parts, filename)
                if (*parts, key) in collision_tracker:
                    # There is already a filepath that maps to this key!
                    warnings.warn(
                        COLLISION_WARNING.format(
                            filepath=filepath,
                            existing=[str(p) for p in collision_tracker[(*parts, key)]],
                            key=key,
                        )
                    )
                    index[parts].remove(key)
                else:
                    try:
                        reader_factory = _reader_factory_for_file(
                            merged_readers_by_mimetype,
                            merged_mimetypes_by_file_ext,
                            mimetype_detection_hook,
                            Path(root, filename),
                        )
                    except NoReaderAvailable:
                        pass
                    else:
                        index[parts].set(key, reader_factory)
                        if greedy:
                            index[parts][key]
                collision_tracker[(*parts, key)].add(filepath)
        # Appending any object will cause bool(initial_scan_complete) to
        # evaluate to True.
        initial_scan_complete.append(object())
        return cls(
            index[()],
            directory=directory,
            index=index,
            subdirectory_trie=subdirectory_trie,
            subdirectory_handler=subdirectory_handler,
            watcher_thread_kill_switch=watcher_thread_kill_switch,
            manual_trigger=manual_trigger,
            metadata=metadata,
            sorting=sorting,
            specs=specs,
            principal=principal,
            access_policy=access_policy,
            entries_stale_after=entries_stale_after,
            metadata_stale_after=metadata_stale_after,
            # The __init__ of this class does not accept any other
            # kwargs, but subclasses can use this to set up additional
            # instance state.
            **kwargs,
        )

    def __init__(
        self,
        mapping,
        directory,
        index,
        subdirectory_trie,
        subdirectory_handler,
        watcher_thread_kill_switch,
        manual_trigger,
        metadata,
        sorting,
        specs,
        access_policy,
        principal,
        entries_stale_after=None,
        metadata_stale_after=None,
        must_revalidate=True,
    ):
        super().__init__(
            mapping,
            metadata=metadata,
            sorting=sorting,
            specs=specs,
            access_policy=access_policy,
            principal=principal,
            entries_stale_after=entries_stale_after,
            metadata_stale_after=metadata_stale_after,
            must_revalidate=must_revalidate,
        )
        self._directory = directory
        self._index = index
        self._watcher_thread_kill_switch = watcher_thread_kill_switch
        self._manual_trigger = manual_trigger
        self._subdirectory_trie = subdirectory_trie
        self._subdirectory_handler = subdirectory_handler

    def update_now(self):
        "Force an update and block until it completes."
        event = threading.Event()
        self._manual_trigger.put(event)
        # The worker thread will set this Event when processing completes.
        # Wait on that, and the return.
        event.wait()

    @property
    def directory(self):
        return self._directory

    def new_variation(self, *args, **kwargs):
        return super().new_variation(
            *args,
            watcher_thread_kill_switch=self._watcher_thread_kill_switch,
            manual_trigger=self._manual_trigger,
            directory=self._directory,
            index=self._index,
            subdirectory_trie=self._subdirectory_trie,
            subdirectory_handler=self._subdirectory_handler,
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
    subdirectory_trie,
    subdirectory_handler,
    readers_by_mimetype,
    mimetypes_by_file_ext,
    mimetype_detection_hook,
    key_from_filename,
    initial_scan_complete,
    watcher_thread_kill_switch,
    manual_trigger,
    greedy,
    collision_tracker,
    poll_interval,
):
    watcher = RegExpWatcher(directory, re_files=ignore_re_files, re_dirs=ignore_re_dirs)
    queued_changes = []
    event = None
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
                    mimetype_detection_hook,
                    key_from_filename,
                    index,
                    subdirectory_trie,
                    subdirectory_handler,
                    greedy,
                    collision_tracker,
                )
            # Process changes just collected.
            _process_changes(
                changes,
                directory,
                readers_by_mimetype,
                mimetypes_by_file_ext,
                mimetype_detection_hook,
                key_from_filename,
                index,
                subdirectory_trie,
                subdirectory_handler,
                greedy,
                collision_tracker,
            )
        else:
            # The initial scan is still going. Stash the changes for later.
            queued_changes.extend(changes)
        if event is not None:
            # The processing above was the result of a manual trigger.
            # Confirm to the sender that it has now completed.
            event.set()
        try:
            event = manual_trigger.get(timeout=poll_interval)
        except queue.Empty:
            event = None


def _process_changes(
    changes,
    directory,
    readers_by_mimetype,
    mimetypes_by_file_ext,
    mimetype_detection_hook,
    key_from_filename,
    index,
    subdirectory_trie,
    subdirectory_handler,
    greedy,
    collision_tracker,
):
    ignore = set()
    # Map adapter for a subdirectory to list of (Change, Path) pairs
    # that it should be notified about.
    to_notify = collections.defaultdict(list)
    for kind, entry in changes:
        path = Path(entry)
        if path in ignore:
            # We have seen this before and could not find a Reader for it.
            # Do not try again.
            continue
        rel_path = path.relative_to(directory)
        parent_parts = rel_path.parent.parts
        d = subdirectory_trie
        subdir_parts = []
        for part in rel_path.parts:
            if part not in d:
                within_separately_managed_subdir = False
                break
            if not isinstance(d[part], dict):
                subdir_parts.append(part)
                adapater = d[part]
                within_separately_managed_subdir = True
                break
            d = d[part]
            subdir_parts.append(part)
        else:
            adapater = d
            assert not isinstance(d, dict)
            within_separately_managed_subdir = True
        if within_separately_managed_subdir:
            to_notify[adapater].append(
                (kind, Path(*rel_path.parts[len(subdir_parts) :]))  # noqa: E203
            )
            continue
        if kind == Change.added:
            if path.is_dir():
                _new_subdir(
                    index,
                    subdirectory_trie,
                    subdirectory_handler,
                    directory,
                    parent_parts,
                    path.name,
                    greedy,
                )
            else:
                key = key_from_filename(path.name)
                if collision_tracker.get((*parent_parts, key), False):
                    # The collision tracker contains a nonempty set.
                    # There is already a filepath that maps to this key!
                    warnings.warn(
                        COLLISION_WARNING.format(
                            filepath=rel_path,
                            existing=[
                                str(p) for p in collision_tracker[(*parent_parts, key)]
                            ],
                            key=key,
                        )
                    )
                    index[parent_parts].remove(key)
                else:
                    # We may observe the creation of a file before we observe the creation of its
                    # directory.
                    if parent_parts not in index:
                        for i in range(len(parent_parts)):
                            if parent_parts[: 1 + i] not in index:
                                _new_subdir(
                                    index,
                                    subdirectory_trie,
                                    subdirectory_handler,
                                    directory,
                                    parent_parts[:i],
                                    parent_parts[i],
                                    greedy,
                                )
                    try:
                        reader_factory = _reader_factory_for_file(
                            readers_by_mimetype,
                            mimetypes_by_file_ext,
                            mimetype_detection_hook,
                            path,
                        )
                    except NoReaderAvailable:
                        # Ignore this file in the future.
                        # We already know that we do not know how to find a Reader
                        # for this filename.
                        ignore.add(path)
                    else:
                        index[parent_parts].set(key, reader_factory)
                        if greedy:
                            index[parent_parts][key]
                    collision_tracker[(*parent_parts, key)].add(rel_path)
        elif kind == Change.deleted:
            if path.is_dir():
                index.pop(parent_parts, None)
            else:
                key = key_from_filename(path.name)
                index[parent_parts].discard(key)
                collision_tracker[(*parent_parts, key)].discard(rel_path)
                if len(collision_tracker[(*parent_parts, key)]) == 1:
                    # A key collision was resolved by the removal (or renaming)
                    # of a conflicting file.
                    (rel_path_with_newly_unique_key,) = collision_tracker[
                        (*parent_parts, key)
                    ]
                    collision_tracker[(*parent_parts, key)].clear()
                    # Process the remaining file which now has a key that is unique.
                    _process_changes(
                        [(Change.added, directory / rel_path_with_newly_unique_key)],
                        directory,
                        readers_by_mimetype,
                        mimetypes_by_file_ext,
                        mimetype_detection_hook,
                        key_from_filename,
                        index,
                        subdirectory_trie,
                        subdirectory_handler,
                        greedy,
                        collision_tracker,
                    )
        elif kind == Change.modified:
            if path.is_dir():
                # Nothing to do with a "modified" directory
                pass
            else:
                key = key_from_filename(path.name)
                # Why do we need a try/except here? A reasonable question!
                # Normally, we would learn about the file first via a Change.added
                # or via the initial scan. Then, later, when we learn about modification
                # we can be sure that we already know how to find a Reader for this
                # filename. But, during that initial scan, there is a race condition
                # where we might learn about Change.modified before we first add that file
                # to our index. Therefore, we guard this with a try/except, knowing
                # that this could be the first time we see this path.
                try:
                    reader_factory = _reader_factory_for_file(
                        readers_by_mimetype,
                        mimetypes_by_file_ext,
                        mimetype_detection_hook,
                        path,
                    )
                except NoReaderAvailable:
                    # Ignore this file in the future.
                    # We already know that we do not know how to find a Reader
                    # for this filename.
                    ignore.add(path)
                else:
                    index[parent_parts].set(key, reader_factory)
                    if greedy:
                        index[parent_parts][key]
    for adapter, changes in to_notify.items():
        print(changes)
        if hasattr(adapter, "get_changes_callback"):
            changes_callback = adapter.get_changes_callback()
            if changes_callback is not None:
                changes_callback(changes)


def _reader_factory_for_file(
    readers_by_mimetype, mimetypes_by_file_ext, mimetype_detection_hook, path
):
    # First, try to infer the mimetype from the file extension.
    # For compound suffixes like '.u1.strict_disabled.avif' (a real example)
    # consider in order:
    # '.u1.strict_disabled.avif'
    # '.strict_disabled.avif'
    # '.avif'
    for i in range(len(path.suffixes)):
        ext = "".join(path.suffixes[i:])  # e.g. ".h5" or ".tar.gz"
        if ext in mimetypes_by_file_ext:
            mimetype = mimetypes_by_file_ext[ext]
            break
    else:
        # Use the Python's built-in facility for guessing mimetype
        # from file extension. This loads data about mimetypes from
        # the operating system the first time it is used.
        mimetype, _ = mimetypes.guess_type(str(path))
    # Finally, user-specified function has the opportunity to
    # look at more than just the file extension. This gets access to the full
    # path, so it can consider the file name and even open the file. It is also
    # passed the mimetype determined above, or None if no match was found.
    if mimetype_detection_hook is not None:
        mimetype = mimetype_detection_hook(path, mimetype)
    if mimetype is None:
        msg = (
            f"The file at {path} has some type that is not "
            "recognized. The file will be skipped. Pass in a mimetype "
            "for this file extension via the parameter "
            "DirectoryAdapter.from_directory(..., mimetypes_by_file_ext={...}) "
            "or a function for determining the mimetype based the full filepath "
            "DirectoryAdapter.from_directory(..., mimetype_detection_hook=func)."
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
            "the parameter DirectoryAdapter.from_directory(..., readers_by_mimetype={...})."
        )
        warnings.warn(msg)
        raise NoReaderAvailable
    return functools.partial(reader_class, str(path))


class NoReaderAvailable(Exception):
    pass


def _new_subdir(
    index,
    subdirectory_trie,
    subdirectory_handler,
    root,
    parent_parts,
    subdirectory,
    greedy,
):
    "make a new mapping and a corresponding tree for this subdirectory."
    # Check whether this subdirectory should be separately managed as a whole,
    # or treated as individual files.
    if subdirectory_handler is not None:
        adapter = subdirectory_handler(Path(root, *parent_parts, subdirectory))
    else:
        adapter = None
    if adapter is None:
        # Process the files in this directory individually.
        mapping = CachingMap(
            {}, cache=cachetools.LRUCache(maxsize=MAX_ADAPTER_CACHE_SIZE)
        )
        index[parent_parts + (subdirectory,)] = mapping
        index[parent_parts].set(subdirectory, functools.partial(MapAdapter, mapping))
        if greedy:
            index[parent_parts][subdirectory]
    else:
        # Hand off management of this directory to the handler.
        d = subdirectory_trie
        for part in parent_parts:
            if part not in d:
                d[part] = {}
            d = d[part]
        d[subdirectory] = adapter
        index[parent_parts].set(subdirectory, lambda: adapter)
        if greedy:
            index[parent_parts][subdirectory]


COLLISION_WARNING = (
    "The file {filepath!s} maps to the key {key!r}. "
    "which collides with the file(s) {existing!r}. "
    "All files that map to this key will be ignored until the collision is resolved. "
    "To resolve this do one of the following: "
    "(1) Use the full, unique filename as the key, "
    "via the commandline flag --keep-ext "
    "or the configurable argument "
    "key_from_filename: 'tiled.adapters.files:identity'  # Use full filename as key "
    "(2) Remove or rename one of the files. "
    "(3) Use a custom key_from_filename that "
    "generates unique keys for this case."
)
