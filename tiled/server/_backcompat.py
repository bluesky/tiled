"""Server-side back-compatibility helpers.

Utilities for adapting outgoing responses (and interpreting incoming requests)
so that older ``python-tiled`` clients keep working when the server evolves.
"""

from typing import Optional

import packaging.version
from fastapi import Request

# Fields added to `tiled.structures.data_source.Asset` after a given version.
# Older python-tiled clients unpack asset dicts as dataclass kwargs and crash
# on unknown fields, so we strip these fields from responses to clients older
# than the version listed here.
ASSET_FIELDS_ADDED_IN = {
    "size": packaging.version.parse("0.2.13"),
}


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
