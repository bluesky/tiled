def _ensure_shared_package_on_path() -> None:
    try:
        import bluesky_authentication  # noqa: F401

        return
    except ModuleNotFoundError:
        pass

    import sys
    from pathlib import Path

    candidate = Path(__file__).resolve().parents[3] / "bluesky-authentication" / "src"
    if candidate.exists():
        sys.path.insert(0, str(candidate))


_ensure_shared_package_on_path()


try:
    from bluesky_authentication.protocols import (
        ExternalAuthenticator,
        InternalAuthenticator,
        UserSessionState,
    )
except ModuleNotFoundError:
    from abc import ABC
    from dataclasses import dataclass
    from typing import Optional

    from fastapi import Request

    @dataclass
    class UserSessionState:
        """Data transfer class to communicate custom session state information."""

        user_name: str
        state: dict = None

    class InternalAuthenticator(ABC):
        def authenticate(self, username: str, password: str) -> Optional[UserSessionState]:
            raise NotImplementedError

    class ExternalAuthenticator(ABC):
        def authenticate(self, request: Request) -> Optional[UserSessionState]:
            raise NotImplementedError
