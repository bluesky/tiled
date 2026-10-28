"""EXPERIMENTAL: the asynchronous `bytes` client.

`AsyncBytesClient` mirrors `tiled.client.bytes.BytesClient`: a `bytes`
node has no content endpoint and no specialized read path, so the client
exists to identify a node as `bytes` for dispatch and to provide an
informative `__repr__`. (The synchronous `raw_export` has no async
counterpart in this first version.)
"""

from __future__ import annotations

from ..bytes import CommonBytesClient
from .base import AsyncBaseClient


class AsyncBytesClient(CommonBytesClient, AsyncBaseClient):
    pass
