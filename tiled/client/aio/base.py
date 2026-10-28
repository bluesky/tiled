"""EXPERIMENTAL: the common behavior of all asynchronous clients.

`AsyncBaseClient` mirrors `tiled.client.base.BaseClient` over the same
protocol layer. Mapping-style dunders and IPython integration are
deliberately absent (they cannot await). Dask, pickling, websockets, and
the HTTP response cache are not part of the first async version.
"""

from __future__ import annotations

from ...structures.data_source import DataSource
from ..base import CommonBaseClient


class AsyncBaseClient(CommonBaseClient):
    async def refresh(self):
        "Refetch the item describing this node. Returns self."
        content = (await self.context.send(self._draft_request_refresh())).json()
        self._merge_refreshed_item(content)
        return self

    async def include_data_sources(self):
        "Return a refreshed copy of this client that includes data source information."
        if self._include_data_sources:
            return self
        return await self.new_variation(include_data_sources=True).refresh()

    async def data_sources(self):
        "The data sources that back this node, fetched on first use."
        client = await self.include_data_sources()
        data_sources_json = client.item["attributes"].get("data_sources")
        if data_sources_json is None:
            return None
        return [DataSource.from_json(d) for d in data_sources_json]

    async def update_metadata(
        self, metadata=None, specs=None, access_tags=None, *, drop_revision=False
    ):
        """
        EXPERIMENTAL: Update metadata via a `dict.update`-like patch (see the
        synchronous `update_metadata` for details and `DELETE_KEY` semantics).
        """
        metadata_patch, specs_patch, access_tags_patch = self.build_metadata_patches(
            metadata, specs, access_tags
        )
        await self.patch_metadata(
            metadata_patch=metadata_patch,
            specs_patch=specs_patch,
            access_tags_patch=access_tags_patch,
            drop_revision=drop_revision,
        )

    async def patch_metadata(
        self,
        metadata_patch=None,
        specs_patch=None,
        access_tags_patch=None,
        content_type="application/json-patch+json",
        drop_revision=False,
    ):
        "EXPERIMENTAL: Patch metadata using a JSON patch or a merge patch."
        self._cached_len = None
        draft = self._draft_request_patch_metadata(
            metadata_patch, specs_patch, access_tags_patch, content_type, drop_revision
        )
        content = (await self.context.send(draft)).json()
        self._merge_patched_metadata(
            content, metadata_patch, specs_patch, access_tags_patch, content_type
        )

    async def replace_metadata(
        self, metadata=None, specs=None, access_tags=None, drop_revision=False
    ):
        "EXPERIMENTAL: Replace metadata entirely."
        self._cached_len = None
        draft = self._draft_request_replace_metadata(
            metadata, specs, access_tags, drop_revision
        )
        content = (await self.context.send(draft)).json()
        self._merge_replaced_metadata(content, metadata, specs, access_tags)

    async def asset_manifest(self, data_sources):
        """
        Return a manifest of the relative paths of the contents in each asset.

        Assets marked `is_directory` are mapped to a list of relative paths;
        other assets are mapped to None.
        """
        manifests = {}
        for data_source in data_sources:
            for asset in data_source.assets:
                if asset.is_directory:
                    draft = self._draft_request_read_asset_manifest(asset.id)
                    manifest = (await self.context.send(draft)).json()["manifest"]
                else:
                    manifest = None
                manifests[asset.id] = manifest
        return manifests

    async def delete(self, recursive: bool = False, external_only: bool = True) -> None:
        """Delete the node and its contents, if any.

        Parameters
        ----------
        recursive : bool, optional
            If True, descend into sub-nodes and delete their contents too.
            Defaults to False.
        external_only : bool, optional
            If True, only delete externally-managed data. Defaults to True.
        """
        self._cached_len = None
        await self.context.send(self._draft_request_delete(recursive, external_only))

    async def close_stream(self) -> None:
        "Declare the end of a stream of writes to this node."
        await self.context.send(self._draft_request_close_stream())
