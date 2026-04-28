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

import httpx
import uvicorn

from ..storage import SQLStorage, get_storage
from ..utils import ensure_uri

_STARTUP_TIMEOUT = 20  # seconds; used for both socket listen and readiness checks


class ThreadedServer(uvicorn.Server):
    def install_signal_handlers(self):
        pass

    @contextlib.contextmanager
    def run_in_thread(self):
        self.thread = threading.Thread(target=self.run)
        self.thread.start()
        try:
            # Wait for server to start up, or raise TimeoutError.
            for _ in range(int(_STARTUP_TIMEOUT / 0.1)):
                time.sleep(0.1)
                if self.started:
                    break
            else:
                raise TimeoutError(
                    f"Server did not start in {_STARTUP_TIMEOUT} seconds."
                )
            host, port = self.servers[0].sockets[0].getsockname()
            yield f"http://{host}:{port}"
        finally:
            self.should_exit = True
            self.thread.join()


class SimpleTiledServer:
    """
    Run a simple Tiled server on a background thread.

    This is intended to be used for tutorials and development. It employs only
    basic security and should not be used to store anything important. It does
    not scale to large number of users. By default, it uses temporary storage.

    Parameters
    ----------
    directory : Optional[Path, str]
        Location where data, including files and embedded databases, will be
        stored. By default, a temporary directory will be used.
    api_key : Optional[str]
        By default, an 8-bit random secret is generated. (Production Tiled
        servers use longer secrets.)
    port : Optional[int]
        Port the server will listen on. By default, a random free high port
        is allocated by the operating system.
    readable_storage : Optional[Union[str, pathlib.Path, list[Union[str, pathlib.Path]]]
        If provided, the server will be able to read from these storage locations, in addition
        to the default storage location defined by `directory`.
    enable_webhooks : bool
        If True, mount the webhooks API and start the webhook dispatcher.
        HTTP targets and local addresses are accepted (no HTTPS or SSRF
        checks), making it easy to test against a local receiver.
        A ``webhook_secret_key`` is auto-generated and exposed as an
        attribute.  Default is False.

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
        readable_storage: Optional[
            Union[str, pathlib.Path, list[Union[str, pathlib.Path]]]
        ] = None,
        enable_webhooks: bool = False,
    ):
        # Delay import to avoid circular import.
        from ..catalog import from_uri as catalog_from_uri
        from ..config import Authentication, StreamingCacheConfig, WebhooksConfig
        from .app import build_app
        from .logging_config import LOGGING_CONFIG
        from .webhook_router import _noop_url_validator

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

        # If webhooks are enabled, generate a secret key for encrypting HMAC
        # signing secrets at rest.  Use a full 32-byte key here since it is
        # stored on disk and not typed by humans.
        webhook_secret_key = secrets.token_hex(32) if enable_webhooks else None

        # Alter copy of default LOGGING_CONFIG to log to files instead of
        # stdout and stderr.
        log_config = copy.deepcopy(LOGGING_CONFIG)
        log_config["handlers"]["access"]["class"] = "logging.FileHandler"
        del log_config["handlers"]["access"]["stream"]
        log_config["handlers"]["access"]["filename"] = str(directory / "access.log")
        log_config["handlers"]["default"]["class"] = "logging.FileHandler"
        del log_config["handlers"]["default"]["stream"]
        log_config["handlers"]["default"]["filename"] = str(directory / "error.log")

        # Catalog from uri wants readable storage to be a list,
        # but we want to allow users to pass in a single path (as a str or pathlib.Path)
        # for convenience.
        if readable_storage is not None and (
            isinstance(readable_storage, str)
            or isinstance(readable_storage, pathlib.Path)
        ):
            readable_storage = [readable_storage]

        self.catalog = catalog_from_uri(
            directory / "catalog.db",
            writable_storage=[directory / "data", storage_uri],
            init_if_not_exists=True,
            readable_storage=readable_storage,
            cache_config=StreamingCacheConfig(uri="memory").model_dump(),
        )
        server_settings = {}
        if enable_webhooks:
            webhook_cfg = WebhooksConfig(secret_keys=[webhook_secret_key])
            server_settings["webhooks"] = webhook_cfg
        self.app = build_app(
            self.catalog,
            authentication=Authentication(single_user_api_key=api_key),
            server_settings=server_settings,
            webhook_url_validator=_noop_url_validator if enable_webhooks else None,
        )
        self._server = ThreadedServer(
            uvicorn.Config(self.app, port=port, loop="asyncio", log_config=log_config)
        )
        self._cm = self._server.run_in_thread()
        base_url = self._cm.__enter__()

        # ThreadedServer.started is True as soon as uvicorn opens the
        # socket, but FastAPI does not serve requests until the lifespan
        # startup_event() completes. Poll /healthz to wait for that.
        # Ensure the background server thread is shut down if anything
        # in the readiness probe fails; otherwise it would leak.
        try:
            deadline = time.monotonic() + _STARTUP_TIMEOUT
            with httpx.Client(trust_env=False) as client:
                while True:
                    if not self._server.thread.is_alive():
                        raise RuntimeError(
                            "Tiled server thread exited before the application "
                            "became ready. Check the log files in the server's "
                            f"directory for details: {directory}"
                        )
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise TimeoutError(
                            "Tiled server started listening but the application "
                            f"did not become ready within {_STARTUP_TIMEOUT} seconds."
                        )
                    try:
                        r = client.get(
                            f"{base_url}/healthz",
                            timeout=min(1.0, remaining),
                        )
                        if r.status_code == 200:
                            break
                    except httpx.RequestError:
                        pass
                    time.sleep(min(0.1, max(0.0, deadline - time.monotonic())))
        except BaseException:
            self._cm.__exit__(None, None, None)
            raise

        # Extract port from base_url.
        actual_port = urlparse(base_url).port

        # Stash attributes for easy introspection
        self.port = actual_port
        self.directory = directory
        self.storage = cast(SQLStorage, get_storage(storage_uri))
        self.api_key = api_key
        self.webhook_secret_key = webhook_secret_key  # None if webhooks not enabled
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
