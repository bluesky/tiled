"""EXPERIMENTAL: the asynchronous array client.

`AsyncArrayClient` mirrors `tiled.client.array.ArrayClient` over the same
protocol layer, returning in-memory numpy arrays (there is no dask
integration, and no public block-wise access -- the synchronous block
methods exist to support dask). Reads that exceed `RESPONSE_BYTESIZE_LIMIT`
are split along chunk boundaries, exactly as in the synchronous client, and fetched
concurrently with `asyncio.gather`, bounded by the context's connection limit.
"""

from __future__ import annotations

import asyncio
from typing import Any, Union

from numpy.typing import NDArray

from ...structures.core import structure_from_json
from .._protocol.array import ArrayReadRecipe
from .._protocol.request import RequestDraft
from ..array import CommonArrayClient
from ..utils import translating_patch_conflicts
from .base import AsyncBaseClient


class AsyncArrayClient(CommonArrayClient, AsyncBaseClient):
    "Async client-side wrapper around an array-like, returning in-memory arrays."

    async def _fetch_one(
        self, draft: RequestDraft, expected_shape: tuple[int, ...]
    ) -> NDArray[Any]:
        content = (await self.context.send(draft)).read()
        return ArrayReadRecipe.parse_response(content, self.dtype, expected_shape)

    async def read(self, slice: Any = None) -> NDArray[Any]:
        """Access the entire array or a slice of it.

        A read that is expected to exceed `RESPONSE_BYTESIZE_LIMIT` is split along
        chunk boundaries and fetched concurrently.
        """
        recipe = self._read_recipe(slice)
        # Zero fetches for a zero-size array, one for a small read, several
        # for a large one; `assemble` handles each case.
        fetched = await asyncio.gather(
            *(
                self._fetch_one(draft, recipe.shape_of_draft(index))
                for index, draft in recipe.drafts.items()
            )
        )
        return recipe.assemble(dict(zip(recipe.drafts, fetched)))

    async def write(self, array: Any, persist: bool = True) -> None:
        await self.context.send(self._draft_request_write(array, persist))

    async def _write_block(self, array: Any, block, persist: bool = True) -> None:
        """Overwrite one block (chunk) of the array.

        Private: block-wise access exists to serve chunked uploads
        (`AsyncContainer.write_array` fans out one write per chunk); the
        public block methods of the synchronous client exist to support
        dask, which the async client does not offer.
        """
        await self.context.send(
            self._draft_request_write_block(array, block, ..., persist)
        )

    async def patch(
        self,
        array: NDArray[Any],
        offset: Union[int, tuple[int, ...]],
        extend: bool = False,
        persist: bool = True,
    ) -> None:
        """Write data into a slice of an array, maybe extending the shape.

        See the synchronous `ArrayClient.patch` for details.
        """
        draft = self._draft_request_patch(array, offset, extend, persist)
        with translating_patch_conflicts():
            response = await self.context.send(draft)
        # Update the cached structure.
        self._structure = structure_from_json(self.structure_family, response.json())
