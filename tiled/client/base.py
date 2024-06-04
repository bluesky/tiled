import time
import warnings
from copy import copy, deepcopy
from dataclasses import asdict
from pathlib import Path

import json_merge_patch
import jsonpatch
import orjson
from httpx import URL

from ..structures.core import STRUCTURE_TYPES, Spec, StructureFamily
from ..structures.data_source import DataSource
from ..utils import UNCHANGED, DictView, ListView, patch_mimetypes, safe_json_dump
from .metadata_update import apply_update_patch
from .utils import MSGPACK_MIME_TYPE, handle_error


class MetadataRevisions:
    def __init__(self, context, link):
        self._cached_len = None
        self.context = context
        self._link = link

    def __len__(self):
        LENGTH_CACHE_TTL = 1  # second

        now = time.monotonic()
        if self._cached_len is not None:
            length, deadline = self._cached_len
            if now < deadline:
                # Used the cached value and do not make any request.
                return length

        content = handle_error(
            self.context.http_client.get(
                self._link,
                headers={"Accept": MSGPACK_MIME_TYPE},
                params={"page[offset]": 0, "page[limit]": 0},
            )
        ).json()
        length = content["meta"]["count"]
        self._cached_len = (length, now + LENGTH_CACHE_TTL)
        return length

    def __getitem__(self, item_):
        self._cached_len = None

        if isinstance(item_, int):
            offset = item_
            limit = 1

            content = handle_error(
                self.context.http_client.get(
                    self._link,
                    headers={"Accept": MSGPACK_MIME_TYPE},
                    params={"page[offset]": offset, "page[limit]": limit},
                )
            ).json()
            (result,) = content["data"]
            return result

        elif isinstance(item_, slice):
            offset = item_.start
            if offset is None:
                offset = 0
            if item_.stop is None:
                params = f"?page[offset]={offset}"
            else:
                limit = item_.stop - offset
                params = f"?page[offset]={offset}&page[limit]={limit}"

            next_page = self._link + params
            result = []
            while next_page is not None:
                content = handle_error(
                    self.context.http_client.get(
                        next_page,
                        headers={"Accept": MSGPACK_MIME_TYPE},
                    )
                ).json()
                if len(result) == 0:
                    result = content.copy()
                else:
                    result["data"].append(content["data"])
                next_page = content["links"]["next"]

            return result["data"]

    def delete_revision(self, n):
        handle_error(self.context.http_client.delete(self._link, params={"number": n}))


class BaseClient:
    # The HTTP spec does not define a size limit for URIs,
    # but a common setting is 4K or 8K (for all the headers together).
    # As another reference point, Internet Explorer imposes a
    # 2048-character limit on URLs.
    URL_CHARACTER_LIMIT = 2_000  # number of characters

    def __init__(
        self,
        context,
        *,
        item,
        structure_clients,
        structure=None,
        include_data_sources=False,
    ):
        self._context = context
        self._item = item
        self._cached_len = None  # a cache just for __len__
        self.structure_clients = structure_clients
        self._metadata_revisions = None
        self._include_data_sources = include_data_sources
        attributes = self.item["attributes"]
        structure_family = attributes["structure_family"]

        if structure is not None:
            # Allow the caller to optionally hand us a structure that is already
            # parsed from a dict into a structure dataclass.
            self._structure = structure
        elif structure_family == StructureFamily.container:
            self._structure = None
        else:
            structure_type = STRUCTURE_TYPES[attributes["structure_family"]]
            self._structure = structure_type.from_json(attributes["structure"])

        super().__init__()

    def structure(self):
        """
        Return a dataclass describing the structure of the data.
        """
        if getattr(self._structure, "resizable", None):
            # In the future, conditionally fetch updated information.
            raise NotImplementedError(
                "The server has indicated that this has a dynamic, resizable "
                "structure and this version of the Tiled Python client cannot "
                "cope with that."
            )
        return self._structure

    def login(self, username=None, provider=None):
        """
        Depending on the server's authentication method, this will prompt for username/password:

        >>> c.login()
        Username: USERNAME
        Password: <input is hidden>

        or prompt you to open a link in a web browser to login with a third party:

        >>> c.login()
        You have ... minutes visit this URL

        https://...

        and enter the code: XXXX-XXXX
        """
        self.context.login(username=username, provider=provider)

    def logout(self):
        """
        Log out.

        This method is idempotent: if you are already logged out, it will do nothing.
        """
        self.context.logout()

    def __repr__(self):
        return f"<{type(self).__name__}>"

    @property
    def context(self):
        return self._context

    def refresh(self):
        content = handle_error(
            self.context.http_client.get(
                self.uri,
                headers={"Accept": MSGPACK_MIME_TYPE},
                params={"include_data_sources": self._include_data_sources},
            )
        ).json()
        self._item = content["data"]
        return self

    @property
    def item(self):
        "JSON payload describing this item. Mostly for internal use."
        return self._item

    @property
    def metadata(self):
        "Metadata about this data source."
        # Ensure this is immutable (at the top level) to help the user avoid
        # getting the wrong impression that editing this would update anything
        # persistent.
        return DictView(self._item["attributes"]["metadata"])

    def metadata_copy(self):
        """
        Generate a mutable copy of metadata and specs for validating metadata
        (useful with update_metadata())
        """
        metadata = deepcopy(self._item["attributes"]["metadata"])
        specs = [Spec(**spec) for spec in self._item["attributes"]["specs"]]
        return [metadata, specs]  # returning as list of mutable items

    @property
    def specs(self):
        "List of specifications describing the structure of the metadata and/or data."
        return ListView([Spec(**spec) for spec in self._item["attributes"]["specs"]])

    @property
    def uri(self):
        "Direct link to this entry"
        return self.item["links"]["self"]

    @property
    def structure_family(self):
        "Quick access to this entry"
        return StructureFamily[self.item["attributes"]["structure_family"]]

    def data_sources(self):
        if not self._include_data_sources:
            warnings.warn(
                """Calling include_data_sources().refresh().
To fetch the data sources up front, call include_data_sources() on the
client or pass the optional parameter `include_data_sources=True` to
`from_uri(...)` or similar."""
            )

        data_sources_json = (
            self.include_data_sources().item["attributes"].get("data_sources")
        )
        if data_sources_json is None:
            return None
        return [DataSource.from_json(d) for d in data_sources_json]

    def include_data_sources(self):
        """
        Ensure that data source and asset information is fetched.

        If it has already been fetched, this is a no-op.
        """
        if self._include_data_sources:
            return self  # no op
        return self.new_variation(include_data_sources=True).refresh()

    def new_variation(
        self,
        structure_clients=UNCHANGED,
        include_data_sources=UNCHANGED,
        **kwargs,
    ):
        """
        This is intended primarily for internal use and use by subclasses.
        """
        if structure_clients is UNCHANGED:
            structure_clients = self.structure_clients
        if include_data_sources is UNCHANGED:
            include_data_sources = self._include_data_sources
        return type(self)(
            self.context,
            item=self._item,
            structure_clients=structure_clients,
            include_data_sources=include_data_sources,
            **kwargs,
        )

    def asset_manifest(self, data_sources):
        """
        Return a manifest of the relative paths of the contents in each asset.

        This return a dictionary keyed on asset ID.
        Assets backed by a single file are mapped to None (no manifest).
        Asset backed by a directory of files are mapped to a list of relative paths.

        Parameters
        ----------
        data_sources : dict
            The value returned by ``.data_sources()``. This is passed in explicitly
            to avoid fetching it twice in common usages. It also enables passing in
            a subset of the data_sources of interest.
        """
        manifests = {}
        for data_source in data_sources:
            manifest_link = self.item["links"]["self"].replace(
                "/metadata", "/asset/manifest", 1
            )
            for asset in data_source.assets:
                if asset.is_directory:
                    manifest = handle_error(
                        self.context.http_client.get(
                            manifest_link, params={"id": asset.id}
                        )
                    ).json()["manifest"]
                else:
                    manifest = None
                manifests[asset.id] = manifest
        return manifests

    def raw_export(self, destination_directory=None, max_workers=4):
        """
        Download the raw assets backing this node.

        This may produce a single file or a directory.

        Parameters
        ----------
        destination_directory : Path, optional
            Destination for downloaded assets. Default is current working directory
        max_workers : int, optional
            Number of parallel workers downloading data. Default is 4.

        Returns
        -------
        paths : List[Path]
            Filepaths of exported files
        """
        if destination_directory is None:
            destination_directory = Path.cwd()
        else:
            destination_directory = Path(destination_directory)

        # Import here to defer the import of rich (for progress bar).
        from .download import ATTACHMENT_FILENAME_PLACEHOLDER, download

        urls = []
        paths = []
        data_sources = self.include_data_sources().data_sources()
        asset_manifest = self.asset_manifest(data_sources)
        if len(data_sources) != 1:
            raise NotImplementedError(
                "Export of multiple data sources not yet supported"
            )
        for data_source in data_sources:
            bytes_link = self.item["links"]["self"].replace(
                "/metadata", "/asset/bytes", 1
            )
            for asset in data_source.assets:
                if len(data_source.assets) == 1:
                    # Only one asset: keep the name simple.
                    base_path = destination_directory
                else:
                    # Multiple assets: Add a subdirectory named for the asset
                    # id to namespace each asset.
                    base_path = Path(destination_directory, str(asset.id))
                if asset.is_directory:
                    relative_paths = asset_manifest[asset.id]
                    urls.extend(
                        [
                            URL(
                                bytes_link,
                                params={
                                    "id": asset.id,
                                    "relative_path": relative_path,
                                },
                            )
                            for relative_path in relative_paths
                        ]
                    )
                    paths.extend(
                        [
                            Path(base_path, relative_path)
                            for relative_path in relative_paths
                        ]
                    )
                else:
                    urls.append(URL(bytes_link, params={"id": asset.id}))
                    paths.append(Path(base_path, ATTACHMENT_FILENAME_PLACEHOLDER))
        return download(self.context.http_client, urls, paths, max_workers=max_workers)

    @property
    def formats(self):
        "List formats that the server can export this data as."
        formats = set()
        for spec in self.item["attributes"]["specs"]:
            formats.update(self.context.server_info["formats"].get(spec["name"], []))
        formats.update(
            self.context.server_info["formats"][
                self.item["attributes"]["structure_family"]
            ]
        )
        return sorted(formats)

    def update_metadata(self, metadata=None, specs=None):
        """
        EXPERIMENTAL: Update metadata via a `dict.update`- like interface.

        `update_metadata` is a user-friendly wrapper for `patch_metadata`.
        This is subject to change or removal without notice.

        Parameters
        ----------
        metadata : dict, optional
            User metadata. May be nested. Must contain only basic types
            (e.g. numbers, strings, lists, dicts) that are JSON-serializable.
        specs : List[str], optional
            List of names that are used to label that the data and/or metadata
            conform to some named standard specification.

        See Also
        --------
        patch_metadata
        replace_metadata

        Notes
        -----
        `update_metadata` constructs a JSON Patch (RFC6902) by comparing user updates
        to existing metadata. It uses a slight variation of JSON Merge Patch (RFC7386)
        as an intermediary to implement a python `dict.update`-like user-friendly
        interface, but with additional features like key deletion (see examples) and
        support for `None (null)` values.

        Examples
        --------

        Add or update a key-value pair at the top or a nested level

        >>> node.update_metadata({'key': new_value})
        >>> node.update_metadata({'top_key': {'nested_key': new_value}})

        Remove an existing key

        >>> from tiled.client.metadata_update import DELETE_KEY
        >>> node.update_metadata({'key_to_be_deleted': DELETE_KEY})

        Interactively update complex metadata using a copy of original structure
        (e.g., in iPython you may use tab completion to navigate nested metadata)

        >>> md = node.metadata_copy()[0]
        >>> md['L1_key']['L2_key']['L3_key'] = new_value  # use tab completion
        >>> md['unwanted_key'] = DELETE_KEY
        >>> node.update_metadata(metadata=md)  # Update the copy on the server
        """
        if isinstance(metadata, list) and len(metadata) == 2:
            if specs is None:
                # Likely [metadata, specs] form from node.metadata_copy()
                metadata, specs = metadata
            else:
                raise ValueError("Duplicate specs provided after [metadata, specs]")

        metadata_patch, specs_patch = self.build_metadata_patches(
            metadata=metadata, specs=specs
        )
        self.patch_metadata(metadata_patch=metadata_patch, specs_patch=specs_patch)

    def build_metadata_patches(self, metadata=None, specs=None):
        """
        Build valid JSON Patches (RFC6902) for metadata and metadata validation
        specs accepted by `patch_metadata`.

        Parameters
        ----------
        metadata : dict, optional
            User metadata. May be nested. Must contain only basic types
            (e.g. numbers, strings, lists, dicts) that are JSON-serializable.

        specs : list[Spec], optional
            Metadata validation specifications.

        Returns
        -------
        metadata_patch : list[dict]
            A JSON serializable object representing a valid JSON patch (RFC6902)
            for metadata.
        specs_patch : list[dict]
            A JSON serializable object representing a valid JSON patch (RFC6902)
            for metadata validation specifications.

        See Also
        --------
        patch_metadata
        update_metadata

        Notes
        -----
        `build_metadata_patch` constructs a JSON Patch (RFC6902) by comparing user updates
        to existing metadata/specs. It uses a slight variation of JSON Merge Patch (RFC7386)
        as an intermediary to implement a python `dict.update`-like user-friendly
        interface, but with additional features like key deletion (see examples) and
        support for `None (null)` values.

        Examples
        --------

        Build a patch for adding/updating a key-value pair at the top or a nested level

        >>> patches = node.build_metadata_patches({'key': new_value})
        >>> patches = node.build_metadata_patches({'top_key': {'nested_key': new_value}})

        Build patches for metadata and specs ("mp", "sp")

        >>> mp, sp = node.build_metadata_patches(metadata=metadata, specs=specs)

        Build a patch for removing an existing key

        >>> from tiled.client.metadata_update import DELETE_KEY
        >>> node.build_metadata_patches({'key_to_be_deleted': DELETE_KEY})

        Interactively build a patch for complex metadata (e.g., in iPython you may use
        tab completion to navigate nested metadata)

        >>> md = node.metadata_copy()[0]
        >>> md['L1_key']['L2_key']['L3_key'] = new_value  # use tab completion
        >>> md['unwanted_key'] = DELETE_KEY
        >>> node.build_metadata_patches(metadata=md)  # Generate the patch
        """

        if metadata is None:
            metadata_patch = []
        else:
            md_copy = deepcopy(self._item["attributes"]["metadata"])
            metadata_patch = jsonpatch.JsonPatch.from_diff(
                self._item["attributes"]["metadata"],
                apply_update_patch(md_copy, metadata),
                dumps=orjson.dumps,
            ).patch

        if specs is None:
            specs_patch = None
        else:
            sp_copy = [spec["name"] for spec in self._item["attributes"]["specs"]]
            specs_patch = (
                []
                if specs is None
                else jsonpatch.JsonPatch.from_diff(
                    sp_copy, specs, dumps=orjson.dumps
                ).patch
            )

        return metadata_patch, specs_patch

    def _build_json_patch(self, origin, update_patch):
        """
        Lower level method to construct a JSON patch from an origin and update_patch.
        An "update_patch" is a `dict.update`-like specification that may include
        `DELETE_KEY` for marking a dictionary key for deletion.
        """
        if update_patch is None:
            return []
        patch = jsonpatch.JsonPatch.from_diff(
            origin, apply_update_patch(origin, update_patch), dumps=orjson.dumps
        )
        return patch.patch

    def _build_metadata_revisions(self):
        if self._metadata_revisions is None:
            link = self.item["links"]["self"].replace("/metadata", "/revisions", 1)
            self._metadata_revisions = MetadataRevisions(self.context, link)

        return self._metadata_revisions

    def patch_metadata(
        self,
        metadata_patch=None,
        specs_patch=None,
        content_type=patch_mimetypes.JSON_PATCH,
    ):
        """
        EXPERIMENTAL: Patch metadata using a JSON Patch (RFC6902).

        This is subject to change or removal without notice.

        Parameters
        ----------
        metadata_patch : List[dict], optional
            JSON-serializable patch to be applied to metadata
        specs_patch : List[dict], optional
            JSON-serializable patch to be applied to metadata validation
            specifications list
        content_type : str
            Mimetype of the patches. Acceptable values are:

            * "application/json-patch+json"
              (See https://datatracker.ietf.org/doc/html/rfc6902)
            * "application/merge-patch+json"
              (See https://datatracker.ietf.org/doc/html/rfc7386)

        See Also
        --------
        update_metadata
        replace_metadata
        """

        self._cached_len = None

        def patcher(doc, patch, patch_type):
            # this helper function applies a given type of patch to the document
            # and returns the modified document
            if patch_type == patch_mimetypes.JSON_PATCH:
                return jsonpatch.apply_patch(
                    doc=doc,
                    patch=patch,
                    in_place=False,
                )
            if patch_type == patch_mimetypes.MERGE_PATCH:
                return json_merge_patch.merge(doc, patch)
            raise ValueError(
                f"Unsupported patch type {content_type}. "
                f"Acceptable values are: {', '.join(patch_mimetypes)}."
            )

        assert content_type in patch_mimetypes
        if specs_patch is None:
            normalized_specs_patch = None
        else:
            normalized_specs_patch = []

        if content_type == patch_mimetypes.JSON_PATCH:
            if specs_patch:
                for spec_patch in copy(specs_patch):
                    value = spec_patch.get("value", None)
                    if isinstance(value, str):
                        spec_patch["value"] = asdict(Spec(value))
                    normalized_specs_patch.append(spec_patch)
        elif content_type == patch_mimetypes.MERGE_PATCH:
            if specs_patch:
                for spec in specs_patch:
                    if isinstance(spec, str):
                        spec = Spec(spec)
                    normalized_specs_patch.append(asdict(spec))

        data = {
            "content-type": content_type,
            "metadata": metadata_patch,
            "specs": normalized_specs_patch,
        }

        content = handle_error(
            self.context.http_client.patch(
                self.item["links"]["self"],
                content=safe_json_dump(data),
            )
        ).json()

        if metadata_patch is not None:
            if "metadata" in content:
                # Metadata was accepted and modified by the specs validator on the server side.
                # It is updated locally using the new version.
                self._item["attributes"]["metadata"] = content["metadata"]
            else:
                # Metadata was accepted as it is by the server.
                # It is updated locally with the version submitted by the client.
                self._item["attributes"]["metadata"] = patcher(
                    dict(self.metadata), metadata_patch, content_type
                )

        if specs_patch is not None:
            current_specs = self._item["attributes"]["specs"]
            patched_specs = patcher(current_specs, normalized_specs_patch, content_type)
            self._item["attributes"]["specs"] = patched_specs

    def replace_metadata(self, metadata=None, specs=None):
        """
        EXPERIMENTAL: Replace metadata entirely (see update_metadata).

        This is subject to change or removal without notice.

        Parameters
        ----------
        metadata : dict, optional
            User metadata. May be nested. Must contain only basic types
            (e.g. numbers, strings, lists, dicts) that are JSON-serializable.
        specs : List[str], optional
            List of names that are used to label that the data and/or metadata
            conform to some named standard specification.

        See Also
        --------
        update_metadata
        patch_metadata
        """

        self._cached_len = None

        if specs is None:
            normalized_specs = None
        else:
            normalized_specs = []
            for spec in specs:
                if isinstance(spec, str):
                    spec = Spec(spec)
                normalized_specs.append(asdict(spec))
        data = {
            "metadata": metadata,
            "specs": normalized_specs,
        }

        content = handle_error(
            self.context.http_client.put(
                self.item["links"]["self"], content=safe_json_dump(data)
            )
        ).json()

        if metadata is not None:
            if "metadata" in content:
                # Metadata was accepted and modified by the specs validator on the server side.
                # It is updated locally using the new version.
                self._item["attributes"]["metadata"] = content["metadata"]
            else:
                # Metadata was accepted as it si by the server.
                # It is updated locally with the version submitted buy the client.
                self._item["attributes"]["metadata"] = metadata

        if specs is not None:
            self._item["attributes"]["specs"] = normalized_specs

    @property
    def metadata_revisions(self):
        if self._metadata_revisions is None:
            link = self.item["links"]["self"].replace("/metadata", "/revisions", 1)
            self._metadata_revisions = MetadataRevisions(self.context, link)

        return self._metadata_revisions

    def delete_tree(self):
        endpoint = self.uri.replace("/metadata/", "/nodes/", 1)
        handle_error(self.context.http_client.delete(endpoint))

    def __dask_tokenize__(self):
        return (type(self), self.uri)
