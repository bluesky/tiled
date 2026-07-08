"""Server-side back-compatibility helpers.

Utilities for adapting outgoing responses (and interpreting incoming requests)
so that older ``python-tiled`` clients keep working when the server evolves.
"""

from typing import Optional

import packaging.version
from fastapi import Request


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
