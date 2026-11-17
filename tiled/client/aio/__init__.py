"""EXPERIMENTAL: an asynchronous Tiled client.

This package provides async counterparts to the synchronous client classes,
built on the same pure protocol layer (`tiled.client._protocol`): every
request builder, response parser, and failure classifier is shared; only
the I/O shells differ. The public API shape mirrors the synchronous client
where asyncio allows, with these deliberate differences:

- Connecting is explicit and awaited: `await AsyncContext.connect(...)`,
  or the `from_uri` / `from_app` coroutines. Close the connection by
  closing the client's context (`async with client.context:` or
  `await client.context.aclose()`).

- `AsyncContainer` is not a `Mapping` (dunder protocols cannot await):
  use `await container.get(key)`, `async for key in container`, and the
  `keys()` / `values()` / `items()` async iterators.

- Reads return in-memory structures (numpy, pandas, ...): there is no dask
  integration. Multi-request reads are fetched concurrently with
  `asyncio.gather`, bounded by the context's connection limit.

This first version supports API-key authentication only, and omits the
HTTP response cache, interactive login, progress bars, pickling, and
websocket subscriptions.
"""

from .constructors import from_app, from_context, from_uri
from .context import AsyncContext

__all__ = ["AsyncContext", "from_app", "from_context", "from_uri"]
