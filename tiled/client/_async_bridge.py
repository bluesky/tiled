import asyncio
import queue
import threading
import weakref

import httpx

from .utils import Sentinel


class AsyncClientBridge:
    """
    Run an httpx.AsyncClient on an event loop on its own thread.

    EXPERIMENTAL: This may be removed or change in a backward-incompatible way
    without notice.

    This exposes just the parts of the AsyncClient API that we need.
    Everything is exposed in a synchronous fashion.

    Motivation: Wrapping an ASGI app only works with an async client, by its
    construction. If we want a sync API, we need to bridge sync--async
    somewhere.
    """

    def __init__(self, *, _startup_hook, **client_kwargs):
        self._startup_hook = _startup_hook
        # This will signal that the all the instance state that is defined in the
        # # worker thread (e.g. self._client) has been defined.
        # We will block on it before returning from __init__ to ensure that
        # we never hand the caller a partially-formed object.
        self._instance_state_setup_complete = threading.Event()
        # This queue accepts requets like (callback, method_name, args, kwargs).
        self._queue = queue.Queue()
        self._thread = threading.Thread(
            target=self._worker, args=(client_kwargs,), name="AsyncClient-worker"
        )
        self._thread.start()
        # We could use any object here, but Sentinel is clearer for debugging.
        self._shutdown_sentinel = Sentinel("SHUTDOWN")
        # TODO This finalizer is not working as intended.
        self._finalizer = weakref.finalize(
            self, self._queue.put, self._shutdown_sentinel
        )
        self._instance_state_setup_complete.wait(timeout=1)

    def _worker(self, client_kwargs):

        self._client = httpx.AsyncClient(**client_kwargs)

        async def loop():
            loop = asyncio.get_running_loop()

            await self._startup_hook()

            self._instance_state_setup_complete.set()
            while True:
                # Poll the queue for work to do.
                try:
                    item = self._queue.get_nowait()
                except queue.Empty:
                    await asyncio.sleep(0.01)
                    continue
                if item is self._shutdown_sentinel:
                    break
                callback, method_name, args, kwargs = item
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
        "Submit to the worker. Return the result or raise the exception."
        response_queue = queue.Queue()

        def callback(future):
            try:
                result = future.result()
            except Exception:
                result = future.exception()
            response_queue.put(result)

        self._queue.put((callback, method_name, args, kwargs))
        result = response_queue.get()
        if isinstance(result, Exception):
            raise result
        return result

    def shutdown(self, wait=True):
        # TODO Why isn't this called at Python exit?
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
