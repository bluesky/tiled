"""EXPERIMENTAL: the asynchronous table (dataframe) client.

`AsyncDataFrameClient` mirrors `tiled.client.dataframe.DataFrameClient`
over the same protocol layer, returning in-memory pandas dataframes (there
is no dask integration). A multi-partition read fetches the partitions
concurrently with `asyncio.gather`, bounded by the context's connection
limit.
"""

from __future__ import annotations

import asyncio

from ..dataframe import CommonDataFrameClient
from .base import AsyncBaseClient


class AsyncDataFrameClient(CommonDataFrameClient, AsyncBaseClient):
    async def _fetch_partition(self, partition, columns):
        "Fetch and decode the data for one partition."
        recipe = self._read_partition_recipe(partition, columns)
        content = (await self.context.send(recipe.draft)).read()
        return recipe.parse_response(content)

    async def read_partition(self, partition, columns=None):
        "Read one partition of the table. Optionally select a subset of the columns."
        if not (0 <= partition < self.structure().npartitions):
            raise IndexError(f"partition {partition} out of range")
        return await self._fetch_partition(partition, columns)

    async def read(self, columns=None):
        """Read the entire table. Optionally select a subset of the columns.

        The partitions are fetched concurrently.
        """
        import pandas

        npartitions = self.structure().npartitions
        partitions = await asyncio.gather(
            *(
                self._fetch_partition(partition, columns)
                for partition in range(npartitions)
            )
        )
        if npartitions == 1:
            return partitions[0]
        return pandas.concat(partitions)

    async def write(self, dataframe) -> None:
        await self.context.send(self._draft_request_write(dataframe))

    async def write_partition(self, partition, dataframe) -> None:
        await self.context.send(
            self._draft_request_write_partition(partition, dataframe)
        )

    async def append_partition(self, partition, dataframe) -> None:
        await self.context.send(
            self._draft_request_append_partition(partition, dataframe)
        )
