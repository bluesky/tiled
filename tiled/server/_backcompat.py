"""Server-side back-compatibility helpers.

Utilities for adapting outgoing responses (and interpreting incoming requests)
so that older ``python-tiled`` clients keep working when the server evolves.
"""

from typing import Any, Mapping, Optional

import packaging.version
from fastapi import HTTPException, Request
from starlette.status import HTTP_403_FORBIDDEN, HTTP_422_UNPROCESSABLE_CONTENT

from ..access_control.protocols import PRINCIPAL_TAG_PREFIXES, normalize_access_tags
from ..type_aliases import AccessTags

# Fields added to `tiled.structures.data_source.Asset` after a given version.
# Older python-tiled clients unpack asset dicts as dataclass kwargs and crash
# on unknown fields, so we strip these fields from responses to clients older
# than the version listed here.
ASSET_FIELDS_ADDED_IN = {
    "size": packaging.version.parse("0.2.13"),
}

# Version in which `access_tags` replaced the `access_blob` dict. Clients older
# than this send and expect `access_blob`.
ACCESS_TAGS_ADDED_IN = packaging.version.parse("0.2.19")


class ConflictingAccessFields(ValueError):
    """A request used both `access_tags` and the deprecated `access_blob`."""


def legacy_access_blob_error(exc: ValueError) -> HTTPException:
    """Map a failed `access_blob` translation onto the status the old server used.

    A malformed blob was rejected by the old access policy as 403. Sending both
    spellings at once is a new kind of confusion, so it is a 422.
    """
    if isinstance(exc, ConflictingAccessFields):
        return HTTPException(
            status_code=HTTP_422_UNPROCESSABLE_CONTENT, detail=str(exc)
        )
    return HTTPException(
        status_code=HTTP_403_FORBIDDEN,
        detail=f"Access policy rejects the provided access blob.\n{exc}",
    )


def raw_python_tiled_client_version(request: Request) -> Optional[str]:
    """Return the raw ``<version>`` from a ``python-tiled/<version>`` User-Agent, or None.

    Returns None when the User-Agent header is absent or does not identify a
    Python Tiled client (i.e. it is some other client that we do not need to
    special-case for backward compatibility).
    """
    user_agent = request.headers.get("user-agent", "")
    if not user_agent.startswith("python-tiled/"):
        return None
    _, _, raw_version = user_agent.partition("/")
    return raw_version


def parse_python_tiled_client_version(
    request: Request,
) -> Optional[packaging.version.Version]:
    """Return the parsed version from a ``python-tiled/<version>`` User-Agent, or None.

    Returns None when the User-Agent is missing, is not a Python Tiled client,
    or reports a version that cannot be parsed. Callers that need to
    distinguish "not a Python Tiled client" from "unparseable version" should
    use ``raw_python_tiled_client_version`` and parse the string themselves.
    """
    raw_version = raw_python_tiled_client_version(request)
    if raw_version is None:
        return None
    try:
        return packaging.version.parse(raw_version)
    except Exception:
        return None


def strip_asset_fields_for_client(
    data_sources: list, client_version: Optional[packaging.version.Version]
) -> None:
    """Remove asset fields that the given python-tiled client cannot accept.

    Mutates the assets inside ``data_sources`` in place. A ``client_version`` of
    None means the request did not come from python-tiled, so we leave the
    payload untouched.
    """
    if client_version is None:
        return
    for field, added_in in ASSET_FIELDS_ADDED_IN.items():
        if client_version >= added_in:
            continue
        for ds in data_sources:
            for asset in ds.get("assets", []) or []:
                asset.pop(field, None)


def access_tags_from_access_blob(
    access_blob: Mapping[str, Any],
) -> Optional[AccessTags]:
    """Translate a legacy ``access_blob`` into access tags.

    An empty blob carries no tag information: the old server left ownership to
    the access policy, which is what ``None`` means to ``init_node`` and
    ``modify_node`` today. ``{"tags": []}`` is *not* empty in this sense; it is
    an explicit request to clear the tags, which the policy admits only for
    admins.

    Any other shape -- including the ``{"user": ...}`` that the old server
    generated for user-owned nodes -- is rejected, exactly as the old access
    policy rejected it.
    """
    if not access_blob:
        return None
    if (
        not isinstance(access_blob, Mapping)
        or len(access_blob) != 1
        or "tags" not in access_blob
    ):
        raise ValueError(
            """access_blob must be in the form '{"tags": ["tag1", "tag2", ...]}'\n"""
            f"Received {access_blob=}\n"
            "If this was a merge patch on a user-owned node, use a replace op instead."
        )
    tags = access_blob["tags"]
    if not isinstance(tags, list) or not all(isinstance(tag, str) for tag in tags):
        raise ValueError(
            """access_blob must be in the form '{"tags": ["tag1", "tag2", ...]}'\n"""
            f"Received {access_blob=}"
        )
    return normalize_access_tags(tags)


def access_blob_from_access_tags(access_tags) -> Optional[dict]:
    """Render access tags in the legacy ``access_blob`` shape.

    A principal tag (``user:<id>`` or ``service:<id>``) marks a node that the
    old server described as ``{"user": <id>}``. Everything else is a tag list.
    """
    if access_tags is None:
        return None
    for tag in sorted(access_tags):
        for prefix in PRINCIPAL_TAG_PREFIXES:
            if tag.startswith(prefix):
                return {"user": tag.removeprefix(prefix)}
    return {"tags": sorted(access_tags)}


def client_expects_access_blob(
    client_version: Optional[packaging.version.Version],
) -> bool:
    """Whether this client predates the replacement of access_blob by access_tags.

    Compared on the base version so that a pre-release or development build of
    the release that introduced `access_tags` counts as new. `packaging` sorts
    `0.2.19.dev47` before `0.2.19`, which would otherwise serve the legacy
    dialect to a client that speaks the current one.
    """
    if client_version is None:
        return False
    base_version = packaging.version.parse(client_version.base_version)
    return base_version < ACCESS_TAGS_ADDED_IN


def access_tags_from_patch_request(body, access_tags, apply_patch) -> AccessTags:
    """Resolve access tags from a PATCH body that uses the deprecated spelling.

    The patch document addresses the old `access_blob`, so it is applied to the
    blob rendering of the node's current tags and the result translated back.
    The tags are returned unchanged when the patch does not alter the blob,
    which covers a user-owned node: its blob has no representation in the tag
    vocabulary a client is allowed to send.
    """
    if body.access_tags is not None:
        raise ConflictingAccessFields(
            "Cannot specify both 'access_tags' and the deprecated 'access_blob'."
        )
    access_blob = access_blob_from_access_tags(access_tags)
    patched = apply_patch(access_blob, legacy_access_blob(body))
    if patched == access_blob:
        return access_tags
    patched_tags = access_tags_from_access_blob(patched)
    return access_tags if patched_tags is None else patched_tags


def legacy_access_blob(body) -> Optional[Any]:
    """Return the deprecated ``access_blob`` a client sent, or None if it sent none.

    Gated on ``model_fields_set`` so that reading the field -- and tripping the
    DeprecationWarning that pydantic attaches to it -- happens only for the
    clients that actually still use it.
    """
    if "access_blob" not in body.model_fields_set:
        return None
    return body.access_blob


def access_tags_from_request(body) -> Optional[AccessTags]:
    """Resolve access tags from a request body that uses the deprecated spelling.

    Callers gate this on ``legacy_access_blob``, so the body is known to carry
    an ``access_blob``. Returns ``None`` for an empty blob, which says nothing
    about access and so leaves the decision to the access policy.
    """
    if body.access_tags is not None:
        raise ConflictingAccessFields(
            "Cannot specify both 'access_tags' and the deprecated 'access_blob'."
        )
    return access_tags_from_access_blob(legacy_access_blob(body))
