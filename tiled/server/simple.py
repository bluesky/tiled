import contextlib
import copy
import pathlib
import platform
import secrets
import shutil
import tempfile
import threading
import time
from typing import Optional, Union, cast
from urllib.parse import quote_plus, urlparse

import uvicorn

from ..storage import SQLStorage, get_storage
from ..utils import ensure_uri

_server_is_running = False


class ThreadedServer(uvicorn.Server):
    def install_signal_handlers(self):
        pass

    @contextlib.contextmanager
    def run_in_thread(self):
        global _server_is_running

        if _server_is_running:
            raise RuntimeError(
                "Only one server can be run at a time " "in a given Python process."
            )
        _server_is_running = True
        thread = threading.Thread(target=self.run)
        thread.start()
        try:
            # Wait for server to start up, or raise TimeoutError.
            for _ in range(200):
                time.sleep(0.1)
                if self.started:
                    break
            else:
                raise TimeoutError("Server did not start in 20 seconds.")
            host, port = self.servers[0].sockets[0].getsockname()
            yield f"http://{host}:{port}"
        finally:
            self.should_exit = True
            thread.join()
            _server_is_running = False


class SimpleTiledServer:
    """
    Run a simple Tiled server on a background thread.

    This is intended to be used for tutorials and development. It employs only
    basic security and should not be used to store anything important. It does
    not scale to large number of users. By default, it uses temporary storage.

    Parameters
    ----------
    directory : Optional[Path, str]
        Location where data and embedded databases will be stored.
        By default, a temporary directory will be used.
    api_key : Optional[str]
        By default, an 8-bit random secret is generated. (Production Tiled
        servers use longer secrets.)
    port : Optional[int]
        Port the server will listen on. By default, a random free high port
        is allocated by the operating system.

    Examples
    --------

    Run a server and connect to it.

    >>> from tiled.server import SimpleTiledServer
    >>> from tiled.client import from_uri
    >>> ts = SimpleTiledServer()
    >>> client = from_uri(ts.uri)

    Locate server data, databases, and log files.

    >>> ts.directory

    Run a server with persistent storage that can be reused.

    >>> ts = SimpleTiledServer("my_data/")
    """

    def __init__(
        self,
        directory: Optional[Union[str, pathlib.Path]] = None,
        api_key: Optional[str] = None,
        port: int = 0,
        readable_storage: Optional[Union[str, pathlib.Path]] = None,
    ):
        # Delay import to avoid circular import.
        from ..catalog import from_uri as catalog_from_uri
        from .app import build_app
        from .logging_config import LOGGING_CONFIG

        if directory is None:
            directory = pathlib.Path(tempfile.mkdtemp())
            self._cleanup_directory = True
        else:
            directory = pathlib.Path(directory).resolve()
            self._cleanup_directory = False
        (directory / "data").mkdir(parents=True, exist_ok=True)
        storage_uri = ensure_uri(f"duckdb:///{str(directory / 'storage.duckdb')}")

        # In production we use a proper 32-bit token, but for brevity we
        # use just 8 here. This server only accepts connections on localhost
        # and is not intended for production use, so we think this is an
        # acceptable concession to usability.
        api_key = api_key or secrets.token_hex(8)

        # Alter copy of default LOGGING_CONFIG to log to files instead of
        # stdout and stderr.
        log_config = copy.deepcopy(LOGGING_CONFIG)
        log_config["handlers"]["access"]["class"] = "logging.FileHandler"
        del log_config["handlers"]["access"]["stream"]
        log_config["handlers"]["access"]["filename"] = str(directory / "access.log")
        log_config["handlers"]["default"]["class"] = "logging.FileHandler"
        del log_config["handlers"]["default"]["stream"]
        log_config["handlers"]["default"]["filename"] = str(directory / "error.log")

        self.catalog = catalog_from_uri(
            directory / "catalog.db",
            writable_storage=[directory / "data", storage_uri],
            init_if_not_exists=True,
            readable_storage=readable_storage,
        )
        self.app = build_app(
            self.catalog, authentication={"single_user_api_key": api_key}
        )
        self._cm = ThreadedServer(
            uvicorn.Config(self.app, port=port, loop="asyncio", log_config=log_config)
        ).run_in_thread()
        base_url = self._cm.__enter__()

        # Extract port from base_url.
        actual_port = urlparse(base_url).port

        # Stash attributes for easy introspection
        self.port = actual_port
        self.directory = directory
        self.storage = cast(SQLStorage, get_storage(storage_uri))
        self.api_key = api_key
        self.uri = f"{base_url}/api/v1?api_key={quote_plus(api_key)}"
        self.web_ui_link = f"{base_url}?api_key={quote_plus(api_key)}"

    def __repr__(self):
        return f"<{type(self).__name__} '{self.uri}'>"

    def _repr_html_(self):
        # For Jupyter
        return f"""
<table>
  <tr>
    <td>Web Interface</td>
    <td><a href={self.web_ui_link}>{self.web_ui_link}</a></td>
  </tr>
    <td>API</td>
    <td><code>{self.web_ui_link}</code></td>
  </tr>
</table>"""

    def close(self):
        self.storage.dispose()  # Close the connection to the storage DB
        self._cm.__exit__(None, None, None)
        if self._cleanup_directory and (platform.system() != "Windows"):
            # Windows cannot delete the logfiles because the global Python
            # logging system still has the logfiles open for appending.
            shutil.rmtree(self.directory)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
