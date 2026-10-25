"""EXPERIMENTAL: the asynchronous sparse (COO) array client.

`AsyncSparseClient` mirrors `tiled.client.sparse.SparseClient` over the
same protocol layer, returning `sparse.COO` arrays.
"""

from __future__ import annotations

from ..sparse import CommonSparseClient
from .base import AsyncBaseClient


class AsyncSparseClient(CommonSparseClient, AsyncBaseClient):
    async def read(self, slice=None):
        "Read the entire array, or optionally a slice of it."
        recipe = self._read_recipe(slice)
        content = (await self.context.send(recipe.draft)).read()
        return recipe.parse_response(content)

    async def todense(self, *args, **kwargs):
        "Return a dense numpy array. May be large."
        return (await self.read()).todense(*args, **kwargs)

    async def write(self, coords, data) -> None:
        await self.context.send(self._draft_request_write(coords, data))
