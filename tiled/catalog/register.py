import collections
import logging
import mimetypes
import re
from pathlib import Path

import anyio
import watchfiles

from ..catalog.utils import ensure_uri
from ..server.schemas import Asset, DataSource, Management
from ..structures.core import StructureFamily
from ..utils import import_object
from .mimetypes import DEFAULT_ADAPTERS_BY_MIMETYPE, DEFAULT_MIMETYPES_BY_FILE_EXT
from .utils import get_structure

logger = logging.getLogger(__name__)


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


def default_filter(path):
    "By default, ignore only hidden files."
    return not path.name.startswith(".")


def resolve_mimetype(path, mimetypes_by_file_ext, mimetype_detection_hook=None):
    """
    Given a filepath (file or directory) detect the mimetype.

    If no mimetype could be resolved, return None.
    """
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
    return mimetype


async def register(
    catalog,
    path,
    prefix="/",
    walkers=None,
    readers_by_mimetype=None,
    mimetypes_by_file_ext=None,
    mimetype_detection_hook=None,
    key_from_filename=strip_suffixes,
    filter=None,
    overwrite=True,
):
    "Register a file or directory (recursively)."
    path = Path(path)
    if walkers is None:
        walkers = DEFAULT_WALKERS
    # If parameters come from a configuration file, they are given
    # are given as importable strings, like "package.module:Reader".
    readers_by_mimetype = readers_by_mimetype or {}
    for key, value in list((readers_by_mimetype).items()):
        if isinstance(value, str):
            readers_by_mimetype[key] = import_object(value)
    merged_readers_by_mimetype = collections.ChainMap(
        readers_by_mimetype, DEFAULT_ADAPTERS_BY_MIMETYPE
    )
    if isinstance(key_from_filename, str):
        key_from_filename = import_object(key_from_filename)
    if mimetype_detection_hook is not None:
        mimetype_detection_hook = import_object(mimetype_detection_hook)
    merged_mimetypes_by_file_ext = collections.ChainMap(
        mimetypes_by_file_ext or {}, DEFAULT_MIMETYPES_BY_FILE_EXT
    )
    if filter is None:
        filter = default_filter
    if isinstance(filter, str):
        filter = import_object(filter)
    prefix_parts = [segment for segment in prefix.split("/") if segment]
    for segment in prefix_parts:
        child_catalog = await catalog.lookup_adapter([segment])
        if child_catalog is None:
            key = key_from_filename(segment)
            await catalog.create_node(
                structure_family=StructureFamily.container,
                metadata={},
                key=key,
            )
            child_catalog = await catalog.lookup_adapter([segment])
        catalog = child_catalog
    if path.is_dir():
        # Recursively enter the directory and any subdirectories.
        if overwrite:
            logger.info(f"  Overwriting '/{'/'.join(prefix_parts)}'")
            await catalog.delete_tree()
        await _walk(
            catalog,
            Path(path),
            walkers,
            merged_readers_by_mimetype,
            merged_mimetypes_by_file_ext,
            mimetype_detection_hook,
            key_from_filename,
            filter,
        )
    else:
        await register_single_item(
            catalog,
            path,
            False,
            merged_readers_by_mimetype,
            merged_mimetypes_by_file_ext,
            mimetype_detection_hook,
            key_from_filename,
            filter,
        )


async def _walk(
    catalog,
    path,
    walkers,
    readers_by_mimetype,
    mimetypes_by_file_ext,
    mimetype_detection_hook,
    key_from_filename,
    filter,
):
    "This is the recursive inner loop of walk."
    files = []
    directories = []
    logger.info("  Walking '%s'", path)
    for item in path.iterdir():
        if not filter(item):
            continue
        if item.is_dir():
            directories.append(item)
        else:
            files.append(item)
    for walker in walkers:
        files, directories = await walker(
            catalog,
            path,
            files,
            directories,
            readers_by_mimetype,
            mimetypes_by_file_ext,
            mimetype_detection_hook,
            key_from_filename,
            filter,
        )
    for directory in directories:
        key = key_from_filename(directory.name)
        await catalog.create_node(
            key=key,
            structure_family=StructureFamily.container,
            metadata={},
        )
        child_catalog = await catalog.lookup_adapter([key])
        await _walk(
            child_catalog,
            directory,
            walkers,
            readers_by_mimetype,
            mimetypes_by_file_ext,
            mimetype_detection_hook,
            key_from_filename,
            filter,
        )


async def one_node_per_item(
    catalog,
    path,
    files,
    directories,
    readers_by_mimetype,
    mimetypes_by_file_ext,
    mimetype_detection_hook,
    key_from_filename,
    filter,
):
    "Process each file and directory as mapping to one logical 'node' in Tiled."
    unhandled_files = []
    unhandled_directories = []
    for file in files:
        result = await register_single_item(
            catalog,
            file,
            False,
            readers_by_mimetype,
            mimetypes_by_file_ext,
            mimetype_detection_hook,
            key_from_filename,
            filter,
        )
        if not result:
            unhandled_files.append(file)
    for directory in directories:
        result = await register_single_item(
            catalog,
            directory,
            True,
            readers_by_mimetype,
            mimetypes_by_file_ext,
            mimetype_detection_hook,
            key_from_filename,
            filter,
        )
        if not result:
            unhandled_directories.append(directory)
    return unhandled_files, unhandled_directories


async def register_single_item(
    catalog,
    item,
    is_directory,
    readers_by_mimetype,
    mimetypes_by_file_ext,
    mimetype_detection_hook,
    key_from_filename,
    filter,
):
    "Register a single file or directory as a node."
    unhandled_items = []
    mimetype = resolve_mimetype(item, mimetypes_by_file_ext, mimetype_detection_hook)
    if mimetype is None:
        unhandled_items.append(item)
        if not is_directory:
            logger.info("    SKIPPED: Could not resolve mimetype for '%s'", item)
        return
    if mimetype not in readers_by_mimetype:
        logger.info(
            "    SKIPPED: Resolved mimetype '%s' but no adapter found for '%s'",
            mimetype,
            item,
        )
        unhandled_items.append(item)
        return
    adapter_factory = readers_by_mimetype[mimetype]
    logger.info("    Resolved mimetype '%s' with adapter for '%s'", mimetype, item)
    try:
        adapter = adapter_factory(item)
    except Exception:
        logger.exception("    SKIPPED: Error constructing adapter for '%s'", item)
        return
    key = key_from_filename(item.name)
    return await catalog.create_node(
        key=key,
        structure_family=adapter.structure_family,
        metadata=dict(adapter.metadata),
        data_sources=[
            DataSource(
                mimetype=mimetype,
                structure=get_structure(adapter),
                parameters={},
                management=Management.external,
                assets=[
                    Asset(
                        data_uri=str(ensure_uri(str(item.absolute()))),
                        is_directory=is_directory,
                    )
                ],
            )
        ],
    )


# Matches filename with (optional) non-digits \D followed by digits \d
# and then the file extension .tif or .tiff.
TIFF_SEQUENCE_STEM_PATTERN = re.compile(r"^(\D*)(\d+)\.(?:tif|tiff)$")


async def tiff_sequence(
    catalog,
    path,
    files,
    directories,
    readers_by_mimetype,
    mimetypes_by_file_ext,
    mimetype_detection_hook,
    key_from_filename,
    filter,
):
    """
    Group files in the given directory into TIFF sequences.

    We are looking for any files:
    - with file extension .tif or .tiff
    - with file name ending in a number

    We group these into sorted groups and make one Node for each.
    A group may have one or more items.
    """
    unhandled_directories = directories
    unhandled_files = []
    sequences = collections.defaultdict(list)
    for file in files:
        if file.is_file():
            match = TIFF_SEQUENCE_STEM_PATTERN.match(file.name)
            if match:
                sequence_name, _sequence_number = match.groups()
                sequences[sequence_name].append(file)
                continue
        unhandled_files.append(file)
    mimetype = "multipart/related;type=image/tiff"
    for name, sequence in sorted(sequences.items()):
        logger.info("    Grouped %d TIFFs into a sequence '%s'", len(sequence), name)
        adapter_class = readers_by_mimetype[mimetype]
        key = key_from_filename(name)
        try:
            adapter = adapter_class(*sequence)
        except Exception:
            logger.exception("    SKIPPED: Error constructing adapter for '%s'", name)
            return
        await catalog.create_node(
            key=key,
            structure_family=adapter.structure_family,
            metadata=dict(adapter.metadata),
            data_sources=[
                DataSource(
                    mimetype=mimetype,
                    structure=get_structure(adapter),
                    parameters={},
                    management=Management.external,
                    assets=[
                        Asset(
                            data_uri=str(ensure_uri(str(item.absolute()))),
                            is_directory=False,
                        )
                        for item in sorted(sequence)
                    ],
                )
            ],
        )
    return unhandled_files, unhandled_directories


DEFAULT_WALKERS = [tiff_sequence, one_node_per_item]


async def watch(
    catalog,
    path,
    prefix="/",
    walkers=None,
    readers_by_mimetype=None,
    mimetypes_by_file_ext=None,
    mimetype_detection_hook=None,
    key_from_filename=strip_suffixes,
    filter=None,
    initial_walk_complete_event=None,
):
    if initial_walk_complete_event is None:
        initial_walk_complete_event = anyio.Event()
    ready_event = anyio.Event()
    stop_event = anyio.Event()
    async with anyio.create_task_group() as tg:
        # Begin listening for changes.
        tg.start_soon(
            _watch,
            ready_event,
            initial_walk_complete_event,
            stop_event,
            catalog,
            path,
            prefix,
            walkers,
            readers_by_mimetype,
            mimetypes_by_file_ext,
            mimetype_detection_hook,
            key_from_filename,
            filter,
        )
        await ready_event.wait()
        # We have begun listening for changes.
        # Now do the initial walk.
        await register(
            catalog,
            path,
            prefix,
            walkers,
            readers_by_mimetype,
            mimetypes_by_file_ext,
            mimetype_detection_hook,
            key_from_filename,
            filter,
        )
        # Signal that initial walk is complete.
        # Process any changes that were accumulated during the initial walk.
        await initial_walk_complete_event.set()


async def _watch(
    ready_event,
    initial_walk_complete_event,
    stop_event,
    catalog,
    path,
    prefix="/",
    walkers=None,
    readers_by_mimetype=None,
    mimetypes_by_file_ext=None,
    mimetype_detection_hook=None,
    key_from_filename=strip_suffixes,
    filter=None,
):
    if filter is None:
        filter = default_filter

    def watch_filter(change, path):
        return default_filter(Path(path))

    await ready_event.set()
    backlog = []
    async for batch in watchfiles.awatch(
        path,
        watch_filter=watch_filter,
        yield_on_timeout=True,
        stop_event=stop_event,
        rust_timeout=1000,
    ):
        if (backlog is not None) and batch:
            logger.info(
                "Detected changes, waiting to process until initial walk completes"
            )
            backlog.extend(batch)
        if (backlog is not None) and initial_walk_complete_event.is_set():
            logger.info("Watching for changes in '%s'", path)
            if backlog:
                logger.info(
                    "Processing backlog of changes that occurred during initial walk..."
                )
                await process_changes(backlog)
            backlog = None
        elif batch:
            # We are caught up. Process changes immediately.
            logger.info("Detected changes")
            await process_changes(batch)


async def process_changes(batch):
    for change in batch:
        change_type, change_path = change
        logger.info("  %s '%s'", change_type.name, change_path)
