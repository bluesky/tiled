import collections
import importlib
import logging
import mimetypes
from dataclasses import asdict
from pathlib import Path

from ..catalog.utils import ensure_uri
from ..serialization.dataframe import XLSX_MIME_TYPE
from ..server.schemas import Asset, DataSource, Management
from ..structures.core import StructureFamily
from ..utils import OneShotCachedMap, import_object

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


def resolve_mimetype(path, mimetypes_by_file_ext=None, mimetype_detection_hook=None):
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
    readers_by_mimetype=None,
    mimetypes_by_file_ext=None,
    mimetype_detection_hook=None,
    key_from_filename=strip_suffixes,
):
    # If readers_by_mimetype comes from a configuration file,
    # objects are given as importable strings, like "package.module:Reader".
    readers_by_mimetype = readers_by_mimetype or {}
    for key, value in list((readers_by_mimetype).items()):
        if isinstance(value, str):
            readers_by_mimetype[key] = import_object(value)
    merged_readers_by_mimetype = collections.ChainMap(
        readers_by_mimetype, DEFAULT_READERS_BY_MIMETYPE
    )
    if isinstance(key_from_filename, str):
        key_from_filename = import_object(key_from_filename)
    if mimetype_detection_hook is not None:
        mimetype_detection_hook = import_object(mimetype_detection_hook)
    merged_mimetypes_by_file_ext = collections.ChainMap(
        mimetypes_by_file_ext or {}, DEFAULT_MIMETYPES_BY_FILE_EXT
    )
    prefix_parts = [segment for segment in prefix.split("/") if segment]
    for segment in prefix_parts:
        child = catalog.lookup_adapter([segment])
        if child is None:
            key = key_from_filename(key)
            await catalog.create_node(
                structure_family=StructureFamily.container,
                metadata={},
                key=key,
            )
            child = catalog.lookup_adapter([segment])
        catalog = child
    await _walk(
        catalog,
        Path(path),
        merged_readers_by_mimetype,
        merged_mimetypes_by_file_ext,
        mimetype_detection_hook,
        key_from_filename,
    )


async def _walk(
    catalog,
    path,
    readers_by_mimetype,
    mimetypes_by_file_ext,
    mimetype_detection_hook,
    key_from_filename,
):
    files = []
    subdirectories = []
    logger.info("Walking %s", path)
    for item in path.iterdir():
        if item.is_dir():
            subdirectories.append(item)
        else:
            files.append(item)
    for file in files:
        mimetype = resolve_mimetype(
            file, mimetypes_by_file_ext, mimetype_detection_hook
        )
        if mimetype is None:
            logger.info("Could not resolve mimetype for %s", file)
            continue
        logger.info("Resolved mimetype %s for %s", mimetype, file)
        if mimetype not in readers_by_mimetype:
            logger.info("No adapter found for mimetype %s", mimetype)
            continue
        adapter_factory = readers_by_mimetype[mimetype]
        try:
            adapter = adapter_factory(file)
            key = key_from_filename(file.name)
        except Exception:
            logger.exception("Error adapting %s", file)
        await catalog.create_node(
            key=key,
            structure_family=adapter.structure_family,
            metadata=dict(adapter.metadata),
            data_sources=[
                DataSource(
                    mimetype=mimetype,
                    structure=_get_structure(adapter),
                    parameters={},
                    management=Management.external,
                    assets=[
                        Asset(
                            data_uri=str(ensure_uri(str(file.absolute()))),
                            is_directory=False,
                        )
                    ],
                )
            ],
        )
    for subdirectory in subdirectories:
        key = key_from_filename(key)
        await catalog.create_node(
            structure_family=StructureFamily.container,
            metadata={},
            key=key,
        )
        child = await catalog.lookup_adapter([key])
        await _walk(
            child,
            subdirectory,
            readers_by_mimetype,
            mimetypes_by_file_ext,
            mimetype_detection_hook,
            key_from_filename,
        )


def _get_structure(adapter):
    if hasattr(adapter, "structure"):
        return asdict(adapter.structure())
    elif hasattr(adapter, "microstructure"):
        return {
            "micro": asdict(adapter.microstructure()),
            "macro": asdict(adapter.macrostructure()),
        }
    else:
        return None
