"""Request builders, parsers, and failure logic for operations on one node.

A node's self link (`item["links"]["self"]`) addresses its metadata; sibling
endpoints serve its revisions, assets, and live stream. Most operations here are
single-request: fetching the node metadata (or one of its descendants, by
appending path segments), updating or replacing its metadata, deleting it,
and fetching the manifests of its assets.

Metadata mutations have one decode subtlety: for each submitted field, the
server's response carries the stored value when a server-side validator
modified the submission, and omits it when the submission was stored
as-is. `merge_mutated_attributes` resolves, per field, what the client
should consider current, given the locally-known outcome of the mutation.

Creating a new node is one POST, but the operation spans retry attempts and
a conflict-adoption fallback, so it is described by `CreateNodeRecipe`: the
request draft plus the normalized attributes that must survive across
attempts, with methods for the response post-processing (`merge`), the
adoption guard (`matches_existing`), and the cross-attempt failure
predicates (`request_may_have_reached_server`, `should_try_adopt`).

No I/O happens in this module; the client classes send the drafts and feed
response content back through the matching methods and functions.
"""

from __future__ import annotations

import collections.abc
import uuid
from copy import deepcopy
from dataclasses import asdict, dataclass
from typing import Any, Optional, Sequence, Union

import httpx
import jsonpatch
import orjson
from packaging.version import Version

from ...structures.core import StructureFamily, structure_from_json
from ...structures.data_source import DataSource
from ...type_aliases import JSON
from ...utils import patch_mimetypes, safe_json_dump
from ..metadata_update import apply_update_patch
from ..utils import (
    MSGPACK_MIME_TYPE,
    FailureKind,
    Specs,
    classify_failure,
    normalize_specs,
)
from .request import RequestDraft, RetryPolicy, draft_request_get_item, register_link


def draft_request_refresh_node(
    self_link: str, include_data_sources: bool = False
) -> RequestDraft:
    "Draft the request that refetches the item describing a node."
    params: dict[str, Any] = {}
    if include_data_sources:
        params["include_data_sources"] = include_data_sources
    return draft_request_get_item(self_link, params)


def draft_request_read_asset_manifest(manifest_link: str, asset_id: int) -> RequestDraft:
    """Draft the request that fetches the manifest of one asset, via `/asset/manifest`.

    Only assets marked `is_directory` (backed by a collection of items
    addressed by relative paths) have manifests; the server answers with
    `{"manifest": [<relative paths>]}`.
    """
    return RequestDraft("GET", manifest_link, params={"id": asset_id})


def draft_request_patch_metadata(
    self_link: str,
    *,
    content_type: str,
    metadata_patch: Optional[list],
    specs_patch: Optional[list],
    access_tags_patch: Optional[list],
    drop_revision: bool = False,
) -> RequestDraft:
    """Draft the request that patches a node's metadata, via `PATCH /metadata`.

    `specs_patch` must already be normalized (see
    `tiled.client.utils.normalize_specs_patch`).
    The patch representation (`content_type`) travels in the JSON body; the
    HTTP `Content-Type` describes the body itself.
    """
    assert content_type in patch_mimetypes
    body = {
        "content-type": content_type,
        "metadata": metadata_patch,
        "specs": specs_patch,
        "access_tags": access_tags_patch,
    }
    return RequestDraft(
        "PATCH",
        self_link,
        params={"drop_revision": True} if drop_revision else {},
        headers={"Content-Type": "application/json"},
        content=safe_json_dump(body),
    )


def build_metadata_patches(
    attributes: JSON,
    metadata: Optional[JSON] = None,
    specs: Optional[list] = None,
    access_tags: Optional[list] = None,
) -> tuple[Optional[list], Optional[list], Optional[list]]:
    """Build JSON Patches (RFC 6902) that express updates against `attributes`.

    `metadata` is a `dict.update`-like specification (which may mark keys for
    deletion with `tiled.client.metadata_update.DELETE_KEY`); `specs` and
    `access_tags` are the desired complete lists. Each patch is computed by
    diffing the node's current `attributes` against the updated state; None
    means "do not touch this field".
    """
    if metadata is None:
        metadata_patch = []
    else:
        if not isinstance(metadata, collections.abc.Mapping):
            raise ValueError(
                f"Metadata must serialize as a JSON object, cannot be {type(metadata)}."
            )
        md_copy = deepcopy(attributes["metadata"])
        metadata_patch = jsonpatch.JsonPatch.from_diff(
            attributes["metadata"],
            apply_update_patch(md_copy, metadata),
            dumps=orjson.dumps,
        ).patch

    if specs is None:
        specs_patch = None
    else:
        sp_copy = [spec["name"] for spec in attributes["specs"]]
        specs_patch = jsonpatch.JsonPatch.from_diff(
            sp_copy, specs, dumps=orjson.dumps
        ).patch

    if not access_tags:
        # An empty list of access_tags is a no-op.
        access_tags_patch = None
    else:
        access_tags_copy = list(attributes["access_tags"])
        access_tags_patch = jsonpatch.JsonPatch.from_diff(
            access_tags_copy, access_tags, dumps=orjson.dumps
        ).patch

    return metadata_patch, specs_patch, access_tags_patch


def merge_mutated_attributes(
    content: JSON,
    *,
    metadata: Optional[JSON] = None,
    specs: Optional[list] = None,
    access_tags: Optional[list] = None,
) -> dict[str, Any]:
    """The node attributes to consider current after a metadata mutation.

    The arguments are the locally-known outcomes of the mutation -- the
    replacement values, or the result of applying a patch locally -- with
    None meaning the field was not submitted. Metadata and access tags come
    from the response when present (a server-side validator modified them)
    and from the local outcome otherwise. Specs are always the local
    outcome: the normalized submission is canonical.
    """
    updated: dict[str, Any] = {}
    if metadata is not None:
        updated["metadata"] = content.get("metadata", metadata)
    if specs is not None:
        updated["specs"] = specs
    if access_tags is not None:
        updated["access_tags"] = content.get("access_tags", access_tags)
    return updated


def draft_request_replace_metadata(
    self_link: str,
    *,
    metadata: Optional[JSON],
    specs: Optional[list],
    access_tags: Optional[list],
    drop_revision: bool = False,
) -> RequestDraft:
    """Draft the request that replaces a node's metadata entirely, via `PUT /metadata`.

    `specs` must already be normalized (`normalize_specs`).
    """
    body = {
        "metadata": metadata,
        "specs": specs,
        "access_tags": access_tags,
    }
    return RequestDraft(
        "PUT",
        self_link,
        params={"drop_revision": True} if drop_revision else {},
        headers={"Content-Type": "application/json"},
        content=safe_json_dump(body),
    )


@dataclass(frozen=True)
class CreateNodeRecipe:
    """A recipe for creating one new node: the request plus follow-up scaffold.

    The create is one POST, but the operation spans retry attempts and a
    conflict-adoption fallback, so the recipe carries, alongside the draft,
    the normalized scaffold that the follow-up steps need: `merge` completes
    the item from the server's create response, and `matches_existing`
    guards the adoption of a conflicting node.

    Attributes
    ----------
    draft : RequestDraft
        POST creating the node, via `/metadata` (or `/register` when the
        data sources reference existing assets).
    item : dict
        Client-side item skeleton, completed by `merge` from the server's
        create response.
    key : str
        The new node's key, generated client-side when not supplied so that
        all retries of this request share it. It acts as an idempotency
        token: a 409 Conflict on a retried create may refer to the node
        created by an earlier attempt of the same request (see
        `should_try_adopt` and `matches_existing`).
    structure_family : StructureFamily
        The new node's structure family, as the enum.
    specs : list
        The normalized (`normalize_specs`) specs.
    metadata : dict
        The submitted metadata after the JSON round-trip normalization that
        the request body applies (numpy scalars, datetimes, etc. become
        plain JSON types).
    data_sources : list
        The `DataSource`s submitted with the create.
    """

    draft: RequestDraft
    item: JSON
    key: str
    structure_family: StructureFamily
    specs: list[JSON]
    metadata: JSON
    data_sources: list[DataSource]

    @classmethod
    def for_new_node(
        cls,
        self_link: str,
        path_parts: list[str],
        structure_family: Union[StructureFamily, str],
        data_sources: list[DataSource],
        *,
        key: Optional[str] = None,
        metadata: Optional[JSON] = None,
        specs: Specs = None,
        access_tags: Optional[list[str]] = None,
        library_version: str,
    ) -> "CreateNodeRecipe":
        """A recipe that creates a new node under the container at `self_link`.

        `library_version` is the server's reported library version; it selects
        backward-compatible body construction for older servers.
        """
        metadata = metadata or {}
        structure_family = StructureFamily(structure_family)
        normalized_specs = normalize_specs(specs or [])

        # Backompatibility: if the server is older than 0.2.4,
        # it can not accept the "properties" field in the data source.
        # This can be removed in later releases.
        if Version(library_version) < Version("0.2.4"):
            data_sources_as_dicts = [
                {k: v for k, v in asdict(ds).items() if k != "properties"}
                for ds in data_sources
            ]
        else:
            data_sources_as_dicts = [asdict(ds) for ds in data_sources]

        item = {
            "attributes": {
                "ancestors": path_parts,
                "metadata": metadata,
                "structure_family": structure_family,
                "specs": normalized_specs,
                "data_sources": data_sources_as_dicts,
                "access_tags": access_tags,
            }
        }
        if key is None:
            key = str(uuid.uuid4())
        body = dict(item["attributes"])
        body["id"] = key

        if any(data_source.assets for data_source in data_sources):
            endpoint = register_link(self_link)
        else:
            endpoint = self_link

        # The create flow orchestrates its own retries (it must track, across
        # attempts, whether a failed attempt may have reached the server --
        # see `decide_failure_action`), so the draft itself is single-attempt.
        draft = RequestDraft(
            "POST",
            endpoint,
            headers={
                "Accept": MSGPACK_MIME_TYPE,
                "Content-Type": "application/json",
            },
            content=safe_json_dump(body),
            retry_policy=RetryPolicy.NONE,
        )
        return cls(
            draft=draft,
            item=item,
            key=key,
            structure_family=structure_family,
            specs=normalized_specs,
            metadata=orjson.loads(safe_json_dump(metadata)),
            data_sources=list(data_sources),
        )

    def merge(self, content: JSON) -> JSON:
        """Merge the server's create response `content` into the `item` skeleton.

        Both `self.item` and `content` are modified in place. The returned
        item is ready to dispatch: `attributes["structure"]` holds the final
        structure -- a structure dataclass for the array-like families, or
        the local-contents placeholder for containers. (The server's create
        response never includes a structure; see `PostMetadataResponse`.)
        """
        item = self.item
        if self.structure_family == StructureFamily.container:
            structure: Any = {"contents": None, "count": None}
        else:
            # Only containers can have multiple data_sources right now.
            (data_source,) = self.data_sources
            structure = data_source.structure
            # A data source may carry its structure as a dict; coerce it to
            # the matching dataclass. When we apply type hints and mypy to
            # the client it should be possible to dispense with this.
            if isinstance(structure, dict):
                structure = structure_from_json(self.structure_family, structure)
        item["attributes"]["structure"] = structure

        # If server returned modified metadata update the local copy.
        # Otherwise, use the normalized copy stored on this recipe, in which
        # any special types (e.g. datetimes, numpy arrays) have already been
        # normalized to natively JSON serializable types.
        if "metadata" in content:
            item["attributes"]["metadata"] = content.pop("metadata")
        else:
            item["attributes"]["metadata"] = self.metadata

        # Ditto for data sources
        if "data_sources" in content:
            item["attributes"]["data_sources"] = [
                ds for ds in content.pop("data_sources")
            ]

        # And for access_tags
        if "access_tags" in content:
            item["attributes"]["access_tags"] = content.pop("access_tags")

        # Merge in "id" and "links" returned by the server.
        item.update(content)
        return item

    def matches_existing(self, existing_attrs: JSON) -> Optional[str]:
        """Check whether an existing node matches the create it conflicted with.

        Returns None if the node's attributes match this recipe (same
        structure family, same specs, and the same metadata after the JSON
        round-trip normalization that the recipe stores), or a human-readable
        description of the first mismatch otherwise.
        """
        existing_family = StructureFamily(existing_attrs["structure_family"])
        # The elif chain evaluates lazily, so cheaper checks short-circuit
        # the metadata comparison.
        if existing_family != self.structure_family:
            mismatch = (
                f"structure_family {existing_family!r} != {self.structure_family!r}"
            )
        # Specs compare in order: their order is significant (e.g. client
        # dispatch uses the first recognized spec).
        elif list(existing_attrs.get("specs") or []) != self.specs:
            mismatch = f"specs {existing_attrs.get('specs')!r} != {self.specs!r}"
        elif existing_attrs.get("metadata") != self.metadata:
            mismatch = "metadata differs"
        else:
            mismatch = None
        return mismatch

    @staticmethod
    def request_may_have_reached_server(exc: Exception) -> bool:
        """Whether a failed create attempt may nonetheless have reached the server.

        True when the request was sent but the response was lost (read/write
        transport error), or a proxy/server-side error (5xx) occurred after
        the fact; see `tiled.client.utils.classify_failure`. The create retry
        loop accumulates this over its attempts (once True, it stays True):
        every create carries a key -- supplied or client-generated -- that
        acts as an idempotency token, so if any attempt may have reached the
        server, a later 409 Conflict may refer to the node created by our own
        earlier attempt.
        """
        return classify_failure(exc) in (
            FailureKind.RESPONSE_LOST,
            FailureKind.SERVER_ERROR,
        )

    @staticmethod
    def should_try_adopt(exc: Exception, may_have_reached_server: bool) -> bool:
        """Whether a failed create attempt warrants trying to adopt an existing node.

        True for a 409 Conflict after an earlier attempt may have reached the
        server (`may_have_reached_server`, accumulated by the caller): the
        conflicting node is then most likely our own earlier attempt's work,
        pending the `matches_existing` check. A 409 is never itself a
        may-have-reached-server failure, so it cannot enable its own adoption.
        """
        return (
            may_have_reached_server
            and isinstance(exc, httpx.HTTPStatusError)
            and exc.response.status_code == httpx.codes.CONFLICT
        )


def draft_request_lookup_node(
    self_link: str,
    key_segments: Sequence[str],
    *,
    include_data_sources: bool = False,
) -> RequestDraft:
    """Draft the request that fetches one (possibly nested) entry, via `/metadata`.

    The path segments are appended to the container's own metadata link,
    "jumping" down the tree in a single request without fetching information
    about intermediate parents.
    """
    params: dict[str, Any] = {}
    if include_data_sources:
        params["include_data_sources"] = True
    link = self_link.rstrip("/") + "".join(f"/{key}" for key in key_segments)
    return draft_request_get_item(link, params)


def draft_request_delete_node(
    self_link: str,
    key: str,
    *,
    recursive: bool,
    external_only: bool,
) -> RequestDraft:
    "Draft the request that deletes one child node, via `DELETE /metadata`."
    return RequestDraft(
        "DELETE",
        f"{self_link}/{key}",
        params={"recursive": recursive, "external_only": external_only},
    )
