import contextlib
import pathlib
import tempfile
import threading
import time

import uvicorn

from tiled.catalog import from_uri as catalog_from_uri
from tiled.server.app import build_app


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


class TempTiledServer:
    def __init__(self, port=0, dir_path=None, api_key="secret"):
        if dir_path is None:
            dir_path = pathlib.Path(tempfile.TemporaryDirectory().name)
        else:
            dir_path = pathlib.Path(dir_path)
        dir_path.mkdir(parents=True, exist_ok=True)

        self.catalog = catalog_from_uri(
            dir_path / "catalog.db",
            writable_storage=dir_path / "data",
            init_if_not_exists=True,
        )
        self.app = build_app(
            self.catalog, authentication={"single_user_api_key": api_key}
        )
        self._cm = ThreadedServer(
            uvicorn.Config(self.app, port=port, loop="asyncio")
        ).run_in_thread()

    def run(self):
        return self._cm.__enter__()
