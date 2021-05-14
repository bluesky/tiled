import asyncio
import queue
import threading
import weakref

import httpx

from .utils import Sentinel


class AsyncClientBridge:
    def __init__(self, **client_kwargs):
        self._loop_starting = threading.Event()
        self._queue = queue.Queue()
        self._thread = threading.Thread(target=self._worker, args=(client_kwargs,))
        self._thread.start()
        self._loop_starting.wait(timeout=1)
        # We could use any object here, but Sentinel is clearer for debugging.
        self._shutdown_sentinel = Sentinel("SHUTDOWN")
        self._finalizer = weakref.finalize(
            self, self._queue.put, self._shutdown_sentinel
        )

    def _worker(self, client_kwargs):

        self._client = httpx.AsyncClient(**client_kwargs)

        async def loop():
            loop = asyncio.get_running_loop()

            # Note: This is important. The Tiled server routes are defined lazily on startup.
            await self._client._transport.app.router.startup()

            self._loop_starting.set()
            while True:
                try:
                    item = self._queue.get_nowait()
                except queue.Empty:
                    await asyncio.sleep(0.01)
                    continue
                if item is self._shutdown_sentinel:
                    break
                method_name, args, kwargs, callback = item
                future = asyncio.run_coroutine_threadsafe(
                    self._task(method_name, *args, **kwargs), loop
                )
                future.add_done_callback(callback)

        asyncio.run(loop())

    async def _task(self, method_name, *args, **kwargs):
        method = getattr(self._client, method_name)
        result = await method(*args, **kwargs)
        return result

    def _run(self, method_name, *args, **kwargs):
        response_queue = queue.Queue()

        def callback(future):
            try:
                result = future.result()
            except Exception:
                result = future.exception()
            response_queue.put(result)

        self._queue.put((method_name, args, kwargs, callback))
        result = response_queue.get()
        if isinstance(result, Exception):
            raise result
        return result

    def shutdown(self, wait=True):
        print("shutdown")
        self._queue.put(self._shutdown_sentinel)
        if wait:
            self._thread.join()

    def send(self, *args, **kwargs):
        return self._run("send", *args, **kwargs)

    def build_request(self, *args, **kwargs):
        return self._client.build_request(*args, **kwargs)

    @property
    def base_url(self):
        return self._client.base_url
