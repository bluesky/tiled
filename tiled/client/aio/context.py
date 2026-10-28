"""EXPERIMENTAL: the connection object of the asynchronous client.

`AsyncContext` is the async counterpart of `tiled.client.context.Context`:
it wraps one `httpx.AsyncClient` and is the single I/O choke point through
which every request of every async client instance is sent (`send`). It is
deliberately smaller than its synchronous counterpart: API-key
authentication only, no token refresh, no HTTP response cache, and no
interactive login.

Construction performs no I/O. Use `await AsyncContext.connect(...)` (or the
package-level `from_uri`), which fetches the server description before
handing the context out, preferably as an async context manager:

>>> async with await AsyncContext.connect("https://tiled.example.com") as context:
...     ...
"""

from __future__ import annotations

import asyncio
from typing import Any, Optional, Union

import httpx2 as httpx
import stamina
from pydantic import TypeAdapter

from ..._version import __version__ as tiled_version
from ...schemas import About
from ...utils import UNSET
from .._protocol.request import RequestDraft, RetryPolicy
from ..context import MAX_CONCURRENT_CONNECTIONS
from ..utils import (
    DEFAULT_TIMEOUT_PARAMS,
    MSGPACK_MIME_TYPE,
    TILED_RETRY_ATTEMPTS,
    TILED_RETRY_TIMEOUT,
    TiledResponse,
    handle_error,
    should_retry,
    should_retry_if_unprocessed,
)

USER_AGENT = f"python-tiled/{tiled_version}"


class AsyncContext:
    """Wrap an `httpx.AsyncClient` with API-key auth, throttling, and retries.

    Not constructed directly in typical use; see `connect` and `from_app`.
    """

    def __init__(
        self,
        uri: Union[str, httpx.URL],
        *,
        api_key: Optional[str] = None,
        app: Optional[Any] = None,
        timeout: Optional[httpx.Timeout] = None,
        verify: bool = True,
        max_connections: int = MAX_CONCURRENT_CONNECTIONS,
    ) -> None:
        # The uri is expected to reach the root API route.
        uri = httpx.URL(uri)
        path = uri.path
        if not path.endswith("/"):
            path = f"{path}/"
        self.api_uri = uri.copy_with(path=path, query=None)
        headers = {"user-agent": USER_AGENT}
        if api_key is not None:
            headers["Authorization"] = f"Apikey {api_key}"
        if timeout is None:
            timeout = httpx.Timeout(**DEFAULT_TIMEOUT_PARAMS)
        transport: httpx.AsyncBaseTransport
        if app is not None:
            # Dispatch requests to an in-process ASGI app. Note that the app's
            # lifespan is not run by the transport; see `from_app`.
            transport = httpx.ASGITransport(app=app)
        else:
            transport = httpx.AsyncHTTPTransport(
                verify=verify,
                limits=httpx.Limits(
                    max_connections=max_connections,
                    max_keepalive_connections=max_connections,
                ),
            )
        self.http_client = httpx.AsyncClient(
            transport=transport,
            base_url=self.api_uri,
            headers=headers,
            timeout=timeout,
            follow_redirects=True,
        )
        # Cap the number of concurrent in-flight requests. This is what makes
        # unbounded `asyncio.gather` fan-outs in the clients safe.
        self._semaphore = asyncio.Semaphore(max_connections)
        # Optionally set by `from_app`; entered/exited with this context.
        self._lifespan_context: Optional[Any] = None
        # Fetched by `connect`/`from_app` before the context is handed out.
        self.server_info: Optional[About] = None

    @classmethod
    async def connect(
        cls,
        uri: Union[str, httpx.URL],
        *,
        api_key: Optional[str] = None,
        timeout: Optional[httpx.Timeout] = None,
        verify: bool = True,
        max_connections: int = MAX_CONCURRENT_CONNECTIONS,
    ) -> "AsyncContext":
        """Construct a context and fetch the server description.

        The returned context is ready for use and should be closed when done,
        most easily by using it as an async context manager.
        """
        context = cls(
            uri,
            api_key=api_key,
            timeout=timeout,
            verify=verify,
            max_connections=max_connections,
        )
        try:
            await context._fetch_server_info()
        except BaseException:
            await context.aclose()
            raise
        return context

    @classmethod
    async def from_app(
        cls,
        app: Any,
        *,
        api_key: Any = UNSET,
        uri: str = "http://local-tiled-app/api/v1",
        run_lifespan: bool = True,
    ) -> "AsyncContext":
        """Construct a context around an in-process ASGI app. Primarily for testing.

        Unless `run_lifespan` is False, the app's lifespan (startup/shutdown
        events, which e.g. initialize the catalog database) is entered here
        and exited when this context is closed. Pass `run_lifespan=False`
        when the app's lifespan is already running elsewhere.
        """
        if api_key is UNSET:
            # Extract the single-user API key from the app, as the
            # synchronous `Context.from_app` does.
            from ...server.settings import get_settings

            settings = app.dependency_overrides[get_settings]()
            api_key = settings.single_user_api_key or None
        context = cls(uri, api_key=api_key, app=app)
        if run_lifespan:
            context._lifespan_context = app.router.lifespan_context(app)
            await context._lifespan_context.__aenter__()
        try:
            await context._fetch_server_info()
        except BaseException:
            await context.aclose()
            raise
        return context

    async def _fetch_server_info(self) -> None:
        draft = RequestDraft(
            "GET",
            str(self.api_uri),
            headers={
                "Accept": MSGPACK_MIME_TYPE,
                "Cache-Control": "no-cache, no-store",
            },
        )
        response = await self.send(draft)
        self.server_info = TypeAdapter(About).validate_python(response.json())

    async def __aenter__(self) -> "AsyncContext":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        "Close the HTTP client (and the in-process app's lifespan, if any)."
        await self.http_client.aclose()
        if self._lifespan_context is not None:
            lifespan, self._lifespan_context = self._lifespan_context, None
            await lifespan.__aexit__(None, None, None)

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self.api_uri}>"

    def __reduce__(self):
        raise TypeError(
            "AsyncContext (and the async clients built on it) cannot be "
            "pickled: it owns an asyncio-bound HTTP client tied to the "
            "running event loop. Reconnect in the target process instead."
        )

    async def _send_once(self, draft: RequestDraft) -> httpx.Response:
        "Send one request (one attempt) and check for errors."
        async with self._semaphore:
            response = await self.http_client.request(
                draft.method,
                draft.url,
                params=draft.params,
                headers=draft.headers,
                content=draft.content,
            )
        # Make `.json()` msgpack-aware, as the synchronous transport does.
        response.__class__ = TiledResponse
        return handle_error(response)

    async def send(self, draft: RequestDraft) -> httpx.Response:
        """Send a `RequestDraft`, retrying according to its retry policy.

        The single I/O choke point for async clients: the async counterpart
        of `tiled.client.utils.send_request_draft`. The returned response has
        been vetted by `handle_error`; HTTP errors surface as exceptions
        (after any retries the draft's policy allows). Concurrent sends are
        bounded by the context's connection limit.
        """
        if draft.retry_policy is RetryPolicy.NONE:
            return await self._send_once(draft)
        on = (
            should_retry_if_unprocessed
            if draft.retry_policy is RetryPolicy.IF_UNPROCESSED
            else should_retry
        )
        async for attempt in stamina.retry_context(
            on=on, attempts=TILED_RETRY_ATTEMPTS, timeout=TILED_RETRY_TIMEOUT
        ):
            with attempt:
                response = await self._send_once(draft)
        return response
