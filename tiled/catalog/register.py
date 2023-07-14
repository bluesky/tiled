import collections
import logging
import mimetypes
import re
from pathlib import Path

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


def resolve_mimetype(path, mimetypes_by_file_ext, mimetype_detection_hook=None):
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


async def walk(
    catalog,
    path,
    prefix="/",
    walkers=None,
    readers_by_mimetype=None,
    mimetypes_by_file_ext=None,
    mimetype_detection_hook=None,
    key_from_filename=strip_suffixes,
):
    "Walk a directory recursively and register its contents."
    # If readers_by_mimetype comes from a configuration file,
    # objects are given as importable strings, like "package.module:Reader".
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
    if walkers is None:
        walkers = [tiff_sequence, one_node_per_item]
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
    await _walk(
        catalog,
        Path(path),
        walkers,
        merged_readers_by_mimetype,
        merged_mimetypes_by_file_ext,
        mimetype_detection_hook,
        key_from_filename,
    )


async def _walk(
    catalog,
    path,
    walkers,
    readers_by_mimetype,
    mimetypes_by_file_ext,
    mimetype_detection_hook,
    key_from_filename,
):
    "This is the recursive inner loop of walk."
    files = []
    directories = []
    logger.info("  Walking '%s'", path)
    for item in path.iterdir():
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
):
    "Process each file and directory as mapping to one logical 'node' in Tiled."
    unhandled_files = []
    unhandled_directories = []
    for file in files:
        result = await _handle_single_item(
            catalog,
            file,
            False,
            readers_by_mimetype,
            mimetypes_by_file_ext,
            mimetype_detection_hook,
            key_from_filename,
        )
        if not result:
            unhandled_files.append(file)
    for directory in directories:
        result = await _handle_single_item(
            catalog,
            directory,
            True,
            readers_by_mimetype,
            mimetypes_by_file_ext,
            mimetype_detection_hook,
            key_from_filename,
        )
        if not result:
            unhandled_directories.append(directory)
    return unhandled_files, unhandled_directories


async def _handle_single_item(
    catalog,
    item,
    is_directory,
    readers_by_mimetype,
    mimetypes_by_file_ext,
    mimetype_detection_hook,
    key_from_filename,
):
    "This is the inner loop of one_node_per_item"
    unhandled_items = []
    mimetype = resolve_mimetype(item, mimetypes_by_file_ext, mimetype_detection_hook)
    if mimetype is None:
        logger.info("    SKIPPED: Could not resolve mimetype for '%s'", item)
        unhandled_items.append(item)
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
        logger.exception("    SKIPPED: Error adapting '%s'", item)
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


# This is (optional) non-digits \D followed by digits \d.
TIFF_SEQUENCE_STEM_PATTERN = re.compile(r"^(\D*)(\d+)$")


async def tiff_sequence(
    catalog,
    path,
    files,
    directories,
    readers_by_mimetype,
    mimetypes_by_file_ext,
    mimetype_detection_hook,
    key_from_filename,
):
    unhandled_directories = directories
    unhandled_files = []
    sequences = collections.defaultdict(list)
    for file in files:
        if file.is_file() and (file.suffix in (".tif", ".tiff")):
            match = TIFF_SEQUENCE_STEM_PATTERN.match(file.stem)
            if match:
                sequence_name, _sequence_number = match.groups()
                sequences[sequence_name].append(file)
                continue
        unhandled_files.append(file)
    mimetype = "multipart/related;type=image/tiff"
    for name, sequence in sequences.items():
        adapter_class = readers_by_mimetype[mimetype]
        key = key_from_filename(name)
        adapter = adapter_class(*sequence)
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
