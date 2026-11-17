"""EXPERIMENTAL: the asynchronous awkward-array client.

`AsyncAwkwardClient` mirrors `tiled.client.awkward.AwkwardClient` over the
same protocol layer, returning `awkward.Array` objects.
"""

from __future__ import annotations

from ..awkward import CommonAwkwardClient
from .base import AsyncBaseClient


class AsyncAwkwardClient(CommonAwkwardClient, AsyncBaseClient):
    async def write(self, container) -> None:
        await self.context.send(self._draft_request_write(container))

    async def read(self, slice=...):
        "Read the array, or optionally a slice of it (fetching only touched buffers)."
        recipe = self._read_recipe(slice)
        content = (await self.context.send(recipe.draft)).read()
        projected_array = recipe.parse_response(content)
        return projected_array[slice]
