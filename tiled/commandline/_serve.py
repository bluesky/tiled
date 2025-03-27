import asyncio
import copy
import functools
import os
import re
import tempfile
from abc import ABC
from logging import StreamHandler
from pathlib import Path
from typing import Annotated, List, Optional, Self

import anyio
import uvicorn
from pydantic import AfterValidator, BaseModel, model_validator
from pydantic_settings import CliApp, CliSubCommand, SettingsError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from tiled.adapters.mapping import MapAdapter
from tiled.alembic_utils import stamp_head
from tiled.authn_database.alembic_constants import (
    ALEMBIC_DIR,
    ALEMBIC_INI_TEMPLATE_PATH,
)
from tiled.authn_database.core import initialize_database
from tiled.catalog.adapter import logger as catalog_logger
from tiled.catalog.utils import classify_writable_storage
from tiled.client.constructors import from_uri
from tiled.client.register import identity
from tiled.client.register import logger as register_logger
from tiled.client.register import register, watch
from tiled.config import parse_configs
from tiled.server.app import build_app, print_server_info
from tiled.server.logging_config import LOGGING_CONFIG
from tiled.server.settings import Settings
from tiled.utils import ensure_specified_sql_driver, import_object

from ..catalog import from_uri as catalog_from_uri
from ..client import from_uri as client_from_uri

SQLITE_CATALOG_FILENAME = "catalog.db"
DUCKDB_TABULAR_DATA_FILENAME = "data.duckdb"
DATA_SUBDIRECTORY = "data"


class ServerCommand(ABC, BaseModel):
    host: Annotated[
        str,
        "Bind socket to this host. Use `--host 0.0.0.0` to make the application "
        "available on your local network. IPv6 addresses are supported, for "
        "example: --host `'::'`.",
    ] = "127.0.0.1"
    port: Annotated[int, "Bind to a socket with this port."] = 8000

    verbose: Annotated[
        bool, "Log details of directory traversal and file registration."
    ] = False
    public: Annotated[
        bool,
        "Turns off requirement for API key authentication for reading. "
        "However, the API key is still required for writing, so data cannot be modified even with "
        "this option selected.",
    ] = False
    api_key: Annotated[
        Optional[str],
        "Set the single-user API key. "
        "By default, a random key is generated at startup and printed.",
    ] = None
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
    log_config: Annotated[
        Optional[str], "Custom uvicorn logging configuration file"
    ] = None
    log_timestamps: Annotated[bool, "Include timestamps in log output."] = False
    scalable: Annotated[
        bool,
        "This verifies that the configuration is compatible with scaled (multi-process) deployments.",
    ] = False

    def get_temporary_catalog_directory() -> Path:
        temp_directory = Path(tempfile.TemporaryDirectory().name)
        temp_directory.mkdir()
        return temp_directory

    def get_database(self, database_uri: str) -> AsyncEngine:
        if database_uri is None:
            database_uri = self.get_temporary_catalog_directory()
            database_uri = ensure_specified_sql_driver(database_uri)

        engine = create_async_engine(database_uri)
        asyncio.run(initialize_database(engine))
        stamp_head(ALEMBIC_INI_TEMPLATE_PATH, ALEMBIC_DIR, database_uri)
        return engine

    def setup_log_config(self):
        if self.log_config is None:
            log_config = LOGGING_CONFIG

        if self.log_timestamps:
            log_config = copy.deepcopy(log_config)
            try:
                log_config["formatters"]["access"]["format"] = (
                    "[%(asctime)s.%(msecs)03dZ] "
                    + log_config["formatters"]["access"]["format"]
                )
                log_config["formatters"]["default"]["format"] = (
                    "[%(asctime)s.%(msecs)03dZ] "
                    + log_config["formatters"]["default"]["format"]
                )
            except KeyError:
                print(
                    "The --log-timestamps option is only applicable with a logging "
                    "configuration that, like the default logging configuration, has "
                    "formatters 'access' and 'default'."
                )
                raise SettingsError()
        return log_config

    def build_server(self, tree: MapAdapter) -> uvicorn.Server:
        log_config = self.setup_log_config()

        web_app = build_app(
            tree,
            Settings(
                allow_anonymous_access=self.public, single_user_api_key=self.api_key
            ),
            scalable=self.scalable,
        )
        print_server_info(
            web_app,
            host=self.host,
            port=self.port,
            include_api_key=self.api_key is None,
        )

        config = uvicorn.Config(
            web_app, host=self.host, port=self.port, log_config=log_config
        )
        return uvicorn.Server(config)


class Directory(ServerCommand):
    directory: Annotated[str, "A directory to serve"]
    watch: Annotated[
        bool, "Update catalog when files are added, removed, or changed."
    ] = False
    "Serve a Tree instance from a directory of files."

    def cli_cmd(self) -> None:
        database_dir = self.get_temporary_catalog_directory()
        engine = self.get_database(database_dir)
        asyncio.run(initialize_database(engine))
        stamp_head(ALEMBIC_INI_TEMPLATE_PATH, ALEMBIC_DIR, database_dir)

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
        catalog_adapter = catalog_from_uri(
            ensure_specified_sql_driver(database_dir),
            readable_storage=[database_dir],
            adapters_by_mimetype=adapters_by_mimetype,
        )
        if self.verbose:
            catalog_logger.addHandler(StreamHandler())
            catalog_logger.setLevel("INFO")
            register_logger.addHandler(StreamHandler())
            register_logger.setLevel("INFO")

        server = self.build_server(catalog_adapter)

        async def run_server():
            await server.serve()

        async def wait_for_server():
            "Wait for server to start up, or raise TimeoutError."
            for _ in range(100):
                await asyncio.sleep(0.1)
                if server.started:
                    break
            else:
                raise TimeoutError("Server did not start in 10 seconds.")
            host, port = server.servers[0].sockets[0].getsockname()
            api_url = f"http://{host}:{port}/api/v1/"
            return api_url

        if self.watch:

            async def serve_and_walk():
                server_task = asyncio.create_task(run_server())
                api_url = await wait_for_server()
                # When we add an AsyncClient for Tiled, use that here.
                client = await anyio.to_thread.run_sync(
                    functools.partial(client_from_uri, api_url, api_key=self.api_key)
                )

                print(f"Server is up. Indexing files in {self.directory}...")
                event = anyio.Event()
                asyncio.create_task(
                    watch(
                        client,
                        self.directory,
                        initial_walk_complete_event=event,
                        mimetype_detection_hook=self.mimetype_detection_hook,
                        mimetypes_by_file_ext=mimetypes_by_file_ext,
                        adapters_by_mimetype=adapters_by_mimetype,
                        walkers=self.walkers,
                        key_from_filename=key_from_filename,
                    )
                )
                await event.wait()
                print("Initial indexing complete. Watching for changes...")
                await server_task

        else:

            async def serve_and_walk():
                server_task = asyncio.create_task(run_server())
                api_url = await wait_for_server()
                # When we add an AsyncClient for Tiled, use that here.
                client = await anyio.to_thread.run_sync(
                    functools.partial(client_from_uri, api_url, api_key=self.api_key)
                )

                print(f"Server is up. Indexing files in {self.directory}...")
                await register(
                    client,
                    self.directory,
                    mimetype_detection_hook=self.mimetype_detection_hook,
                    mimetypes_by_file_ext=mimetypes_by_file_ext,
                    adapters_by_mimetype=adapters_by_mimetype,
                    walkers=self.walkers,
                    key_from_filename=key_from_filename,
                )
                print("Indexing complete.")
                await server_task

        asyncio.run(serve_and_walk())


class Catalog(ServerCommand):
    database: Annotated[
        Optional[str], "A filepath or database URI, e.g. 'catalog.db'"
    ] = None
    read: Annotated[
        Optional[List[str]], "Locations that the server may read from"
    ] = None
    write: Annotated[
        Optional[List[str]], "Locations that the server may write to"
    ] = None
    temp: Annotated[bool, "Make a new catalog in a temporary directory."] = False
    init: Annotated[bool, "Initialize a new catalog database."] = False
    scalable: Annotated[
        bool,
        "This verifies that the configuration is compatible with scaled (multi-process) deployments.",
    ] = False
    "Serve a catalog."

    @model_validator(mode="after")
    def temp_or_database(self) -> Self:
        if self.database is not None and self.temp:
            raise ValueError("Expected temp or a database uri but received both.")
        if self.database is None and not self.temp:
            raise ValueError(
                "Database required if not temp- try `tiled admin database init`."
            )
        return self

    def cli_cmd(self) -> None:
        write = self.write or []
        if self.temp and not write:
            temp_directory: Path = self.get_temporary_catalog_directory()
            print(
                f"  writable file storage:     {temp_directory / DATA_SUBDIRECTORY}",
            )
            writable_dir = temp_directory / DATA_SUBDIRECTORY
            writable_dir.mkdir()
            write.append(writable_dir)
            print(
                f"  writable tabular storage:  {temp_directory / DUCKDB_TABULAR_DATA_FILENAME}",
            )
            tabular_data_database = (
                f"duckdb:///{temp_directory / DUCKDB_TABULAR_DATA_FILENAME}"
            )
            write.append(tabular_data_database)
        # TODO Hook into server lifecycle hooks to delete this at shutdown.

        if self.verbose:
            catalog_logger.addHandler(StreamHandler())
            catalog_logger.setLevel("INFO")

        if not write:
            print(
                "This catalog will be served as read-only. "
                "To make it writable, specify a writable directory with --write.",
            )

        tree = from_uri(
            self.database,
            writable_storage=classify_writable_storage(write),
            readable_storage=self.read,
            init_if_not_exists=self.init,
        )
        self.run_server(tree)


class PyObject(ServerCommand):
    object_path: Annotated[
        str, "Object path, as in 'package.subpackage.module:object_name'"
    ]

    "Serve a Tree instance from a Python module."

    def cli_cmd(self) -> None:
        tree = import_object(self.object_path)
        self.run_server(tree)


class Demo(BaseModel):
    host: Annotated[
        str,
        "Bind socket to this host. Use `--host 0.0.0.0` to make the application "
        "available on your local network. IPv6 addresses are supported, for "
        "example: --host `'::'`.",
    ] = "127.0.0.1"
    port: Annotated[int, "Bind to a socket with this port."] = 8000

    """Start a public server with example data."""

    def cli_cmd(self) -> None:
        tree = import_object(self.object_path)
        web_app = build_app(tree, Settings(allow_anonymous_access=True))
        print_server_info(web_app, host=self.host, port=self.port, include_api_key=True)
        uvicorn.run(web_app, host=self.host, port=self.port)


def get_config_path(config_path: Optional[Path]) -> Path:
    if config_path is None:
        return Path(os.getenv("TILED_CONFIG", "config.yml"))
    return config_path


class CheckConfig(BaseModel):
    config_path: Annotated[
        Optional[Path],
        "Path to a config file or directory of config files. "
        "If None, check environment variable TILED_CONFIG. "
        "If that is unset, try default location ./config.yml.",
        AfterValidator(get_config_path),
    ] = None
    "Check configuration file for syntax and validation errors."

    def cli_cmd(self) -> None:
        try:
            parse_configs(self.config_path)
            print("No errors found in configuration.")
        except Exception as err:
            print(str(err), err=True)
            raise SettingsError()


class Config(ServerCommand):
    config_path: Annotated[
        Optional[Path],
        "Path to a config file or directory of config files. "
        "If None, check environment variable TILED_CONFIG. "
        "If that is unset, try default location ./config.yml.",
        AfterValidator(get_config_path),
    ] = None
    "Serve a Tree as specified in configuration file(s)."

    def cli_cmd(self) -> None:
        try:
            settings: Settings = parse_configs(self.config_path)
            self.build_server(settings.tree)
        except Exception as err:
            print(str(err), err=True)
            raise SettingsError()


class Serve(BaseModel):
    directory: CliSubCommand[Directory]
    catalog: CliSubCommand[Catalog]
    demo: CliSubCommand[Demo]
    pyobject: CliSubCommand[PyObject]
    config: CliSubCommand[Config]

    def cli_cmd(self) -> None:
        CliApp.run_subcommand(self)
