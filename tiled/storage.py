import dataclasses
import functools
from pathlib import Path
from typing import Dict, Optional, Union
from urllib.parse import urlparse, urlunparse

from .utils import ensure_uri, path_from_uri

__all__ = [
    "EmbeddedSQLStorage",
    "FileStorage",
    "SQLStorage",
    "Storage",
    "get_storage",
    "parse_storage",
]


@dataclasses.dataclass(frozen=True)
class Storage:
    "Base class for representing storage location"
    uri: str

    def __post_init__(self):
        object.__setattr__(self, "uri", ensure_uri(self.uri))


@dataclasses.dataclass(frozen=True)
class FileStorage(Storage):
    "Filesystem storage location"

    @functools.cached_property
    def path(self):
        return path_from_uri(self.uri)


@dataclasses.dataclass(frozen=True)
class EmbeddedSQLStorage(Storage):
    "File-based SQL database storage location"


@dataclasses.dataclass(frozen=True)
class SQLStorage(Storage):
    "File-based SQL database storage location"
    username: Optional[str] = None
    password: Optional[str] = None

    def __post_init__(self):
        # Extract username, password from URI if given in URI.
        parsed_uri = urlparse(self.uri)
        netloc = parsed_uri.netloc
        if "@" in netloc:
            auth, netloc = netloc.split("@")
            username, password = auth.split(":")
            if (self.username is not None) or (self.password is not None):
                raise ValueError(
                    "Credentials passed both in URI and in username/password fields."
                )
            object.__setattr__(self, "username", username)
            object.__setattr__(self, "password", password)
            # Create clean components with the updated netloc
            clean_components = (
                parsed_uri.scheme,
                netloc,
                parsed_uri.path,
                parsed_uri.params,
                parsed_uri.query,
                parsed_uri.fragment,
            )
            object.__setattr__(self, "uri", urlunparse(clean_components))
        super().__post_init__()

    @functools.cached_property
    def authenticated_uri(self):
        parsed_uri = urlparse(self.uri)
        components = (
            parsed_uri.scheme,
            f"{self.username}:{self.password}@{parsed_uri.netloc}",
            parsed_uri.path,
            parsed_uri.params,
            parsed_uri.query,
            parsed_uri.fragment,
        )

        return urlunparse(components)


def parse_storage(item: Union[Path, str]) -> Storage:
    item = ensure_uri(item)
    scheme = urlparse(item).scheme
    if scheme == "file":
        result = FileStorage(item)
    elif scheme == "postgresql":
        result = SQLStorage(item)
    elif scheme in {"sqlite", "duckdb"}:
        result = EmbeddedSQLStorage(item)
    else:
        raise ValueError(f"writable_storage item {item} has unrecognized scheme")
    return result


# This global registry enables looking up Storage via URI, primarily for the
# purpose of obtaining credentials, which are not stored in the catalog
# database.
_STORAGE: Dict[str, Storage] = {}


def register_storage(storage: Storage) -> None:
    "Stash Storage for later lookup by URI."
    _STORAGE[storage.uri] = storage


def get_storage(uri: str) -> Storage:
    "Look up Storage by URI."
    return _STORAGE[uri]
