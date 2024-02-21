import asyncio
import re
from typing import List

import typer


def register(
    uri: str = typer.Argument(..., help="URL to Tiled node to register on"),
    filepath: str = typer.Argument(..., help="A file or directory to register"),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help=("Log details of directory traversal and file registration."),
    ),
    watch: bool = typer.Option(
        False,
        "--watch",
        "-w",
        help="Update catalog when files are added, removed, or changed.",
    ),
    prefix: str = typer.Option(
        "/", help="Location within the catalog's namespace to register these files"
    ),
    keep_ext: bool = typer.Option(
        False,
        "--keep-ext",
        help=(
            "Serve a file like 'measurements.csv' as its full filepath with extension, "
            "instead of the default which would serve it as 'measurements'. "
            "This is discouraged because it leaks details about the storage "
            "format to the client, such that changing the storage in the future "
            "may break user (client-side) code."
        ),
    ),
    ext: List[str] = typer.Option(
        None,
        "--ext",
        help=(
            "Support custom file extension, mapping it to a known mimetype. "
            "Spell like '.tif=image/tiff'. Include the leading '.' in the file "
            "extension."
        ),
    ),
    mimetype_detection_hook: str = typer.Option(
        None,
        "--mimetype-hook",
        help=(
            "ADVANCED: Custom mimetype detection Python function. "
            "Expected interface: detect_mimetype(filepath, mimetype) -> mimetype "
            "Specify here as 'package.module:function'"
        ),
    ),
    adapters: List[str] = typer.Option(
        None,
        "--adapter",
        help=(
            "ADVANCED: Custom Tiled Adapter for reading a given format"
            "Specify here as 'mimetype=package.module:function'"
        ),
    ),
    walkers: List[str] = typer.Option(
        None,
        "--walker",
        help=(
            "ADVANCED: Custom Tiled Walker for traversing directories and "
            "grouping files. This is used in conjunction with Adapters that operate "
            "on groups of files. "
            "Specify here as 'package.module:function'"
        ),
    ),
    api_key: str = typer.Option(
        None,
        "--api-key",
    ),
):
    if keep_ext:
        from ..client.register import identity

        key_from_filename = identity
    else:
        key_from_filename = None
    mimetypes_by_file_ext = {}
    EXT_PATTERN = re.compile(r"(.*) *= *(.*)")
    for item in ext or []:
        match = EXT_PATTERN.match(item)
        if match is None:
            raise ValueError(
                f"Failed parsing --ext option {item}, expected format '.ext=mimetype'"
            )
        ext, mimetype = match.groups()
        mimetypes_by_file_ext[ext] = mimetype
    adapters_by_mimetype = {}
    ADAPTER_PATTERN = re.compile(r"(.*) *= *(.*)")
    for item in adapters or []:
        match = ADAPTER_PATTERN.match(item)
        if match is None:
            raise ValueError(
                f"Failed parsing --adapter option {item}, expected format 'mimetype=package.module:obj'"
            )
        mimetype, obj_ref = match.groups()
        adapters_by_mimetype[mimetype] = obj_ref

    from ..client import from_uri

    client_node = from_uri(uri, api_key=api_key)

    from logging import StreamHandler

    from ..client.register import logger as register_logger
    from ..client.register import register
    from ..client.register import watch as watch_

    if verbose:
        register_logger.addHandler(StreamHandler())
        register_logger.setLevel("INFO")
    if watch:
        asyncio.run(
            watch_(
                client_node,
                filepath,
                prefix=prefix,
                mimetype_detection_hook=mimetype_detection_hook,
                mimetypes_by_file_ext=mimetypes_by_file_ext,
                adapters_by_mimetype=adapters_by_mimetype,
                walkers=walkers,
                key_from_filename=key_from_filename,
            )
        )
    else:
        asyncio.run(
            register(
                client_node,
                filepath,
                prefix=prefix,
                mimetype_detection_hook=mimetype_detection_hook,
                mimetypes_by_file_ext=mimetypes_by_file_ext,
                adapters_by_mimetype=adapters_by_mimetype,
                walkers=walkers,
                key_from_filename=key_from_filename,
            )
        )
