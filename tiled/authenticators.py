import warnings


def _ensure_shared_package_on_path() -> None:
    try:
        import bluesky_authentication  # noqa: F401

        return
    except ModuleNotFoundError:
        pass

    import sys
    from pathlib import Path

    candidate = Path(__file__).resolve().parents[2] / "bluesky-authentication" / "src"
    if candidate.exists():
        sys.path.insert(0, str(candidate))


_ensure_shared_package_on_path()

warnings.warn(
    "Importing authenticators from 'tiled.authenticators' is deprecated and will be "
    "removed in a future release. Use 'bluesky_authentication.authenticators' and "
    "'bluesky_authentication.protocols' instead.",
    DeprecationWarning,
    stacklevel=2,
)

from bluesky_authentication.authenticators import (  # noqa: F401
    DictionaryAuthenticator,
    DummyAuthenticator,
    EntraAuthenticator,
    LDAPAuthenticator,
    OIDCAuthenticator,
    PAMAuthenticator,
    ProxiedOIDCAuthenticator,
    SAMLAuthenticator,
)
from bluesky_authentication.protocols import (  # noqa: F401
    ExternalAuthenticator,
    InternalAuthenticator,
    UserSessionState,
)

__all__ = [
    "DictionaryAuthenticator",
    "DummyAuthenticator",
    "EntraAuthenticator",
    "ExternalAuthenticator",
    "InternalAuthenticator",
    "LDAPAuthenticator",
    "OIDCAuthenticator",
    "PAMAuthenticator",
    "ProxiedOIDCAuthenticator",
    "SAMLAuthenticator",
    "UserSessionState",
]
