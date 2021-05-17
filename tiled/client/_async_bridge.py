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
            target=self._worker,
            args=(client_kwargs,),
            name="AsyncClient-worker",
            daemon=True,
        )
        self._thread.start()
        # We could use any object here, but Sentinel is clearer for debugging.
        self._shutdown_sentinel = Sentinel("SHUTDOWN")
        # When this AsyncClientBridge is garbage collected, cleanly shutdown the worker.
        self._finalizer = weakref.finalize(
            self, self._queue.put, self._shutdown_sentinel
        )
        # If the worker takes more than 2 seconds to start and set up the
        # instance state something has gone very wrong. Give up.
        # (This is in fact an overly generous timeout, but sometimes
        # in CI environments where resources are constrained and uneven
        # this kinds of overly-long tolerances are needed.)
        self._instance_state_setup_complete.wait(timeout=2)

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
                callback, func, args, kwargs = item
                future = asyncio.run_coroutine_threadsafe(
                    self._task(func, *args, **kwargs), loop
                )
                future.add_done_callback(callback)

        asyncio.run(loop())

    async def _task(self, func, *args, **kwargs):
        result = await func(*args, **kwargs)
        return result

    def _run(self, func, *args, **kwargs):
        "Submit to the worker. Return the result or raise the exception."
        response_queue = queue.Queue()

        def callback(future):
            try:
                result = future.result()
            except Exception:
                result = future.exception()
            response_queue.put(result)

        self._queue.put((callback, func, args, kwargs))
        result = response_queue.get()
        if isinstance(result, Exception):
            raise result
        return result

    def shutdown(self, wait=True):
        # Shutdown worker.
        self._queue.put(self._shutdown_sentinel)
        if wait:
            self._thread.join()

    def send(self, *args, **kwargs):
        return self._run(self._client.send, *args, **kwargs)

    def build_request(self, *args, **kwargs):
        return self._client.build_request(*args, **kwargs)

    def close(self):
        self._run(self._client.aclose)
        self.shutdown()

    @property
    def base_url(self):
        return self._client.base_url
