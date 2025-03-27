import asyncio
import re
from logging import StreamHandler
from typing import Annotated, List, Optional

from pydantic import BaseModel

from tiled.client.constructors import from_uri
from tiled.client.register import identity, logger, register, watch


class Register(BaseModel):
    uri: Annotated[str, "URL to Tiled node to register on"]
    filepath: Annotated[str, "A file or directory to register"]
    verbose: Annotated[
        bool, "Log details of directory traversal and file registration."
    ] = False
    watch: Annotated[
        bool, "Update catalog when files are added, removed, or changed."
    ] = False
    prefix: Annotated[
        str, "Location within the catalog's namespace to register these files"
    ] = "/"
    keep_ext: Annotated[
        bool,
        "Serve a file like 'measurements.csv' as its full filepath with extension, "
        "instead of the default which would serve it as 'measurements'. "
        "This is discouraged because it leaks details about the storage "
        "format to the client, such that changing the storage in the future "
        "may break user (client-side) code.",
    ] = False
    ext: Annotated[
        Optional[List[str]],
        "Support custom file extension, mapping it to a known mimetype. "
        "Spell like '.tif=image/tiff'. Include the leading '.' in the file "
        "extension.",
    ] = None
    mimetype_detection_hook: Annotated[
        Optional[str],
        "ADVANCED: Custom mimetype detection Python function. "
        "Expected interface: detect_mimetype(filepath, mimetype) -> mimetype "
        "Specify here as 'package.module:function'",
    ] = None

    adapters: Annotated[
        Optional[List[str]],
        "ADVANCED: Custom Tiled Adapter for reading a given format"
        "Specify here as 'mimetype=package.module:function'",
    ] = None
    walkers: Annotated[
        Optional[List[str]],
        "ADVANCED: Custom Tiled Walker for traversing directories and "
        "grouping files. This is used in conjunction with Adapters that operate "
        "on groups of files. "
        "Specify here as 'package.module:function'",
    ] = None
    api_key: Optional[str] = None

    def cli_cmd(self) -> None:
        if self.keep_ext:
            key_from_filename = identity
        else:
            key_from_filename = None
        mimetypes_by_file_ext = {}
        EXT_PATTERN = re.compile(r"(.*) *= *(.*)")
        for item in self.ext or []:
            match = EXT_PATTERN.match(item)
            if match is None:
                raise ValueError(
                    f"Failed parsing --ext option {item}, expected format '.ext=mimetype'"
                )
            ext, mimetype = match.groups()
            mimetypes_by_file_ext[ext] = mimetype
        adapters_by_mimetype = {}
        ADAPTER_PATTERN = re.compile(r"(.*) *= *(.*)")
        for item in self.adapters or []:
            match = ADAPTER_PATTERN.match(item)
            if match is None:
                raise ValueError(
                    f"Failed parsing --adapter option {item}, expected format 'mimetype=package.module:obj'"
                )
            mimetype, obj_ref = match.groups()
            adapters_by_mimetype[mimetype] = obj_ref

        client_node = from_uri(self.uri, api_key=self.api_key)

        if self.verbose:
            logger.addHandler(StreamHandler())
            logger.setLevel("INFO")
        if self.watch:
            asyncio.run(
                watch(
                    client_node,
                    self.filepath,
                    prefix=self.prefix,
                    mimetype_detection_hook=self.mimetype_detection_hook,
                    mimetypes_by_file_ext=mimetypes_by_file_ext,
                    adapters_by_mimetype=adapters_by_mimetype,
                    walkers=self.walkers,
                    key_from_filename=key_from_filename,
                )
            )
        else:
            asyncio.run(
                register(
                    client_node,
                    self.filepath,
                    prefix=self.prefix,
                    mimetype_detection_hook=self.mimetype_detection_hook,
                    mimetypes_by_file_ext=mimetypes_by_file_ext,
                    adapters_by_mimetype=adapters_by_mimetype,
                    walkers=self.walkers,
                    key_from_filename=key_from_filename,
                )
            )
