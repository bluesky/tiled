"""EXPERIMENTAL: the asynchronous ragged-array client.

`AsyncRaggedClient` mirrors `tiled.client.ragged.RaggedClient` over the
same protocol layer, returning `ragged.array` objects.
"""

from __future__ import annotations

from typing import Any, Union

import ragged

from ...structures.core import structure_from_json
from ...structures.ragged import RaggedCompatibleType
from ..ragged import CommonRaggedClient
from ..utils import translating_patch_conflicts
from .base import AsyncBaseClient


class AsyncRaggedClient(CommonRaggedClient, AsyncBaseClient):
    async def write(self, array: RaggedCompatibleType, persist=True) -> None:
        "Write a ragged array in full. See the synchronous `RaggedClient.write`."
        await self.context.send(self._draft_request_write(array, persist))

    async def _write_block(
        self,
        array: RaggedCompatibleType,
        block: Union[int, tuple[int, ...]],
        persist=True,
    ) -> None:
        """Overwrite one block of the array.

        Private: block-wise access exists to serve chunked uploads
        (`AsyncContainer.write_ragged` fans out one write per block).
        """
        await self.context.send(self._draft_request_write_block(array, block, persist))

    async def patch(
        self,
        array: RaggedCompatibleType,
        offset: Union[int, tuple[int, ...]],
        extend=False,
        persist=True,
    ) -> None:
        """Write data into a slice of an array, possibly extending the shape.

        See the synchronous `RaggedClient.patch` for details.
        """
        draft = self._draft_request_patch(array, offset, extend, persist)
        with translating_patch_conflicts():
            response = await self.context.send(draft)
        # Update the cached structure.
        self._structure = structure_from_json(self.structure_family, response.json())

    async def read(self, slice: Any | None = None) -> ragged.array:
        "Read the entire array, or optionally a slice of it."
        recipe = self._read_recipe(slice)
        content = (await self.context.send(recipe.draft)).read()
        return recipe.parse_response(content)
