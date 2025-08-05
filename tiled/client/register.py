import collections
import dataclasses
import logging
import mimetypes
import re
from functools import partial
from pathlib import Path

import anyio
import httpx
import watchfiles

from ..mimetypes import (
    DEFAULT_MIMETYPES_BY_FILE_EXT,
    DEFAULT_REGISTRATION_ADAPTERS_BY_MIMETYPE,
)
from ..structures.core import StructureFamily
from ..structures.data_source import Asset, DataSource, Management
from ..utils import ensure_uri, import_object
from .utils import ClientError

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


@dataclasses.dataclass(frozen=True)
class Settings:
    adapters_by_mimetype: dict
    mimetypes_by_file_ext: dict
    mimetype_detection_hook: callable
    key_from_filename: callable
    filter: callable

    @classmethod
    def init(
        cls,
        adapters_by_mimetype=None,
        mimetypes_by_file_ext=None,
        mimetype_detection_hook=None,
        key_from_filename=None,
        filter=None,
    ):
        # If parameters come from a configuration file, they
        # are given as importable strings, like "package.module:Reader".
        adapters_by_mimetype = adapters_by_mimetype or {}
        for key, value in list((adapters_by_mimetype).items()):
            if isinstance(value, str):
                adapters_by_mimetype[key] = import_object(value)
        merged_adapters_by_mimetype = collections.ChainMap(
            adapters_by_mimetype, DEFAULT_REGISTRATION_ADAPTERS_BY_MIMETYPE
        )
        if isinstance(key_from_filename, str):
            key_from_filename = import_object(key_from_filename)
        elif key_from_filename is None:
            key_from_filename = strip_suffixes
        if mimetype_detection_hook is not None:
            mimetype_detection_hook = import_object(mimetype_detection_hook)
        merged_mimetypes_by_file_ext = collections.ChainMap(
            mimetypes_by_file_ext or {}, DEFAULT_MIMETYPES_BY_FILE_EXT
        )
        if filter is None:
            filter = default_filter
        if isinstance(filter, str):
            filter = import_object(filter)
        return cls(
            adapters_by_mimetype=merged_adapters_by_mimetype,
            mimetypes_by_file_ext=merged_mimetypes_by_file_ext,
            mimetype_detection_hook=mimetype_detection_hook,
            key_from_filename=key_from_filename,
            filter=filter,
        )


async def register(
    node,
    path,
    prefix="/",
    walkers=None,
    adapters_by_mimetype=None,
    mimetypes_by_file_ext=None,
    mimetype_detection_hook=None,
    key_from_filename=None,
    filter=None,
    overwrite=True,
):
    "Register a file or directory (recursively)."
    settings = Settings.init(
        adapters_by_mimetype=adapters_by_mimetype,
        mimetypes_by_file_ext=mimetypes_by_file_ext,
        mimetype_detection_hook=mimetype_detection_hook,
        key_from_filename=key_from_filename,
        filter=filter,
    )
    path = Path(path)
    parsed_walkers = []
    for walker in walkers or []:
        parsed_walkers.append(import_object(walker))
    parsed_walkers.extend(DEFAULT_WALKERS)
    prefix_parts = [segment for segment in prefix.split("/") if segment]
    for segment in prefix_parts:
        child_node = await anyio.to_thread.run_sync(node.get, segment)
        if child_node is None:
            key = key_from_filename(segment)
            await create_node_or_drop_collision(
                node,
                structure_family=StructureFamily.container,
                data_sources=[],
                metadata={},
                specs=[],
                key=key,
                access_tags=[],
            )
            # TODO When we have a tiled AsyncClient, use that.
            child_node = await anyio.to_thread.run_sync(node.get, segment)
        node = child_node
    if path.is_dir():
        # Recursively enter the directory and any subdirectories.
        if overwrite:
            logger.info(f"  Overwriting '/{'/'.join(prefix_parts)}'")
            # TODO When we have a tiled AsyncClient, use that.
            await anyio.to_thread.run_sync(partial(node.delete, recursive=True))
        await _walk(
            node,
            Path(path),
            parsed_walkers,
            settings=settings,
        )
    else:
        await register_single_item(
            node,
            path,
            is_directory=False,
            settings=settings,
        )


async def _walk(
    node,
    path,
    walkers,
    settings,
):
    "This is the recursive inner loop of walk."
    files = []
    directories = []
    logger.info("  Walking '%s'", path)
    for item in path.iterdir():
        if not settings.filter(item):
            continue
        if item.is_dir():
            directories.append(item)
        else:
            files.append(item)
    for walker in walkers:
        files, directories = await walker(
            node,
            path,
            files,
            directories,
            settings,
        )
    for directory in directories:
        key = settings.key_from_filename(directory.name)
        await create_node_or_drop_collision(
            node,
            key=key,
            structure_family=StructureFamily.container,
            data_sources=[],
            metadata={},
            specs=[],
            access_tags=[],
        )
        # TODO When we have a tiled AsyncClient, use that.
        child_node = await anyio.to_thread.run_sync(node.get, key)
        await _walk(
            child_node,
            directory,
            walkers,
            settings,
        )


async def one_node_per_item(
    node,
    path,
    files,
    directories,
    settings,
):
    "Process each file and directory as mapping to one logical 'node' in Tiled."
    unhandled_files = []
    unhandled_directories = []
    for file in files:
        result = await register_single_item(
            node,
            file,
            is_directory=False,
            settings=settings,
        )
        if result is None:
            unhandled_files.append(file)
    for directory in directories:
        result = await register_single_item(
            node,
            directory,
            is_directory=True,
            settings=settings,
        )
        if result is None:
            unhandled_directories.append(directory)
    return unhandled_files, unhandled_directories


async def register_single_item(
    node,
    item,
    is_directory,
    settings,
):
    "Register a single file or directory as a node."
    unhandled_items = []
    mimetype = resolve_mimetype(
        item, settings.mimetypes_by_file_ext, settings.mimetype_detection_hook
    )
    if mimetype is None:
        unhandled_items.append(item)
        if not is_directory:
            logger.info("    SKIPPED: Could not resolve mimetype for '%s'", item)
        return
    if mimetype not in settings.adapters_by_mimetype:
        logger.info(
            "    SKIPPED: Resolved mimetype '%s' but no adapter found for '%s'",
            mimetype,
            item,
        )
        unhandled_items.append(item)
        return
    adapter_class = settings.adapters_by_mimetype[mimetype]
    logger.info("    Resolved mimetype '%s' with adapter for '%s'", mimetype, item)
    try:
        adapter = await anyio.to_thread.run_sync(
            adapter_class.from_uris, ensure_uri(item)
        )
    except Exception:
        logger.exception("    SKIPPED: Error constructing adapter for '%s':", item)
        return
    key = settings.key_from_filename(item.name)
    if hasattr(adapter, "generate_data_sources"):
        # Let the Adapter describe the DataSouce(s).
        data_sources = adapter.generate_data_sources(
            mimetype, dict_or_none, item, is_directory
        )
    else:
        # Back-compat: Assume one Asset passed as a
        # parameter named 'data_uri'.
        data_sources = [
            DataSource(
                structure_family=adapter.structure_family,
                mimetype=mimetype,
                structure=dict_or_none(adapter.structure()),
                parameters={},
                management=Management.external,
                assets=[
                    Asset(
                        data_uri=ensure_uri(item),
                        is_directory=is_directory,
                        parameter="data_uri",
                    )
                ],
            )
        ]
    return await create_node_or_drop_collision(
        node,
        key=key,
        structure_family=adapter.structure_family,
        metadata=dict(adapter.metadata()),
        specs=adapter.specs,
        data_sources=data_sources,
    )


# Matches filename with (optional) prefix characters followed by digits \d
# and then the file extension .tif or .tiff.
IMG_SEQUENCE_STEM_PATTERNS = {
    ".tif": re.compile(r"^(.*?)(\d+)\.(?:tif|tiff)$"),
    ".tiff": re.compile(r"^(.*?)(\d+)\.(?:tif|tiff)$"),
    ".jpg": re.compile(r"^(.*?)(\d+)\.(?:jpg|jpeg)$"),
    ".jpeg": re.compile(r"^(.*?)(\d+)\.(?:jpg|jpeg)$"),
    ".npy": re.compile(r"^(.*?)(\d+)\.npy$"),
}
IMG_SEQUENCE_EMPTY_NAME_ROOT = "_unnamed"

IMG_SEQUENCE_MIMETYPES = {
    ".tif": "multipart/related;type=image/tiff",
    ".tiff": "multipart/related;type=image/tiff",
    ".jpg": "multipart/related;type=image/npy",
    ".jpeg": "multipart/related;type=image/jpeg",
    ".npy": "multipart/related;type=application/x-npy",
}


async def group_image_sequences(
    node,
    path,
    files,
    directories,
    settings,
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
        file_ext = Path(file).suffixes[-1] if len(Path(file).suffixes) > 0 else None
        if file.is_file() and file_ext and file_ext in IMG_SEQUENCE_STEM_PATTERNS:
            match = IMG_SEQUENCE_STEM_PATTERNS[file_ext].match(file.name)
            if match:
                sequence_name, _sequence_number = match.groups()
                if sequence_name == "":
                    sequence_name = IMG_SEQUENCE_EMPTY_NAME_ROOT
                sequences[sequence_name].append(file)
                continue
        unhandled_files.append(file)
    for name, sequence in sequences.items():
        await register_image_sequence(node, name, sorted(sequence), settings)
    return unhandled_files, unhandled_directories


async def register_image_sequence(node, name, sequence, settings):
    suffixes = Path(sequence[0]).suffixes
    file_ext = suffixes[-1] if len(suffixes) > 0 else None
    if file_ext and file_ext in IMG_SEQUENCE_MIMETYPES:
        mimetype = IMG_SEQUENCE_MIMETYPES[file_ext]
    else:
        raise RuntimeError(f"Cannot register image sequence {name}.")
    logger.info(
        "    Grouped %d %s images into a sequence '%s'",
        len(sequence),
        mimetype.split("/")[-1],
        name,
    )
    adapter_class = settings.adapters_by_mimetype[mimetype]
    key = settings.key_from_filename(name)
    try:
        adapter = adapter_class.from_uris(
            *[ensure_uri(filepath) for filepath in sequence]
        )
    except Exception:
        logger.exception("    SKIPPED: Error constructing adapter for '%s'", name)
        return
    await create_node_or_drop_collision(
        node,
        key=key,
        structure_family=adapter.structure_family,
        metadata=dict(adapter.metadata()),
        specs=adapter.specs,
        data_sources=[
            DataSource(
                structure_family=adapter.structure_family,
                mimetype=mimetype,
                structure=dict_or_none(adapter.structure()),
                parameters={},
                management=Management.external,
                assets=[
                    Asset(
                        data_uri=ensure_uri(item),
                        is_directory=False,
                        parameter="data_uris",
                        num=i,
                    )
                    for i, item in enumerate(sequence)
                ],
            )
        ],
    )


async def skip_all(
    node,
    path,
    files,
    directories,
    settings,
):
    """
    Skip all files and directories without processing them.

    This can be used to override the DEFAULT_WALKERS.
    """
    for item in files:
        logger.info("    SKIP ALL: Nothing yet handled file '%s'", item)
    return [], directories


DEFAULT_WALKERS = [group_image_sequences, one_node_per_item]


async def watch(
    node,
    path,
    prefix="/",
    walkers=None,
    adapters_by_mimetype=None,
    mimetypes_by_file_ext=None,
    mimetype_detection_hook=None,
    key_from_filename=None,
    filter=None,
    initial_walk_complete_event=None,
):
    settings = Settings.init(
        adapters_by_mimetype=adapters_by_mimetype,
        mimetypes_by_file_ext=mimetypes_by_file_ext,
        mimetype_detection_hook=mimetype_detection_hook,
        key_from_filename=key_from_filename,
        filter=filter,
    )
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
            node,
            path,
            prefix,
            walkers,
            settings,
        )
        await ready_event.wait()
        # We have begun listening for changes.
        # Now do the initial walk.
        await register(
            node,
            path,
            prefix,
            walkers,
            adapters_by_mimetype=settings.adapters_by_mimetype,
            mimetypes_by_file_ext=settings.mimetypes_by_file_ext,
            mimetype_detection_hook=settings.mimetype_detection_hook,
            key_from_filename=settings.key_from_filename,
            filter=settings.filter,
        )
        # Signal that initial walk is complete.
        # Process any changes that were accumulated during the initial walk.
        initial_walk_complete_event.set()


async def _watch(
    ready_event,
    initial_walk_complete_event,
    stop_event,
    node,
    path,
    prefix,
    walkers,
    settings,
):
    def watch_filter(change, path):
        return settings.filter(Path(path))

    ready_event.set()
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
                await process_changes(
                    batch,
                    node,
                    path,
                    prefix,
                    walkers,
                    settings,
                )
            backlog = None
        elif batch:
            # We are caught up. Process changes immediately.
            logger.info("Detected changes")
            await process_changes(
                batch,
                node,
                path,
                prefix,
                walkers,
                settings,
            )


async def process_changes(
    batch,
    node,
    path,
    prefix,
    walkers,
    settings,
):
    for change in batch:
        change_type, change_path = change
        logger.info("  %s '%s'", change_type.name, change_path)
        # TODO Be more selective.
        # We should be able to re-register only a select portion of the
        # full tree. For now, we ignore the change batch content and just
        # use the change as a timing signal to re-register the whole tree.
        await register(
            node,
            path,
            prefix,
            walkers,
            adapters_by_mimetype=settings.adapters_by_mimetype,
            mimetypes_by_file_ext=settings.mimetypes_by_file_ext,
            mimetype_detection_hook=settings.mimetype_detection_hook,
            key_from_filename=settings.key_from_filename,
            filter=settings.filter,
        )


async def create_node_or_drop_collision(
    node,
    *args,
    key,
    **kwargs,
):
    "Call node.new(...) and if there is a collision remove the original."

    # TODO When we have a tiled AsyncClient, use that.
    # run_sync does not accept kwargs, so we make a closure here.
    def new(*args):
        return node.new(*args, key=key, **kwargs)

    try:
        return await anyio.to_thread.run_sync(new, *args)
    except ClientError as err:
        if err.response.status_code == httpx.codes.CONFLICT:
            # To avoid ambiguity include _neither_ the original nor the new one.
            offender = await anyio.to_thread.run_sync(node.get, key)
            await anyio.to_thread.run_sync(partial(offender.delete, recursive=True))
            logger.warning(
                "   COLLISION: Multiple files would result in node at '%s'. Skipping all.",
                err.args[0],
            )
        else:
            raise


def dict_or_none(structure):
    if structure is None:
        return None
    return dataclasses.asdict(structure)
