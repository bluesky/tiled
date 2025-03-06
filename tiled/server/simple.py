import contextlib
import copy
import pathlib
import secrets
import tempfile
import threading
import time
from typing import Optional, Union
from urllib.parse import quote_plus

import uvicorn


class ThreadedServer(uvicorn.Server):
    def install_signal_handlers(self):
        pass

    @contextlib.contextmanager
    def run_in_thread(self):
        thread = threading.Thread(target=self.run)
        thread.start()
        try:
            # Wait for server to start up, or raise TimeoutError.
            for _ in range(100):
                time.sleep(0.1)
                if self.started:
                    break
            else:
                raise TimeoutError("Server did not start in 10 seconds.")
            host, port = self.servers[0].sockets[0].getsockname()
            yield f"http://{host}:{port}"
        finally:
            self.should_exit = True
            thread.join()


class SimpleTiledServer:
    def __init__(
        self,
        port: int = 0,
        dir_path: Optional[Union[str, pathlib.Path]] = None,
        api_key: Optional[str] = None,
    ):
        # Delay import to avoid circular import.
        from ..catalog import from_uri as catalog_from_uri
        from .app import build_app
        from .logging_config import LOGGING_CONFIG

        if dir_path is None:
            dir_path = pathlib.Path(tempfile.TemporaryDirectory(delete=False).name)
        else:
            dir_path = pathlib.Path(dir_path)
        dir_path.mkdir(parents=True, exist_ok=True)
        api_key = api_key or secrets.token_hex(32)

        # Alter copy of default LOGGING_CONFIG to log to files instead of
        # stdout and stderr.
        log_config = copy.deepcopy(LOGGING_CONFIG)
        log_config["handlers"]["access"]["class"] = "logging.FileHandler"
        del log_config["handlers"]["access"]["stream"]
        log_config["handlers"]["access"]["filename"] = str(dir_path / "access.log")
        log_config["handlers"]["default"]["class"] = "logging.FileHandler"
        del log_config["handlers"]["default"]["stream"]
        log_config["handlers"]["default"]["filename"] = str(dir_path / "error.log")

        self.catalog = catalog_from_uri(
            dir_path / "catalog.db",
            writable_storage=dir_path / "data",
            init_if_not_exists=True,
        )
        self.app = build_app(
            self.catalog, authentication={"single_user_api_key": api_key}
        )
        self._cm = ThreadedServer(
            uvicorn.Config(self.app, port=port, loop="asyncio", log_config=log_config)
        ).run_in_thread()
        netloc = self._cm.__enter__()

        # Stash attributes for easy introspection
        self.port = port
        self.dir_path = dir_path
        self.api_key = api_key
        self.uri = f"{netloc}/api/v1?api_key={quote_plus(api_key)}"
        self.web_ui_link = f"{netloc}?api_key={quote_plus(api_key)}"

    def __repr__(self):
        return f"<{type(self).__name__} '{self.uri}'>"

    def _repr_html_(self):
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
        self._cm.__exit__(None, None, None)
