import hashlib
import re
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import canonicaljson

SCHEME_PATTERN = re.compile(r"^[a-z0-9+]+:\/\/.*$")


def ensure_uri(uri_or_path):
    "Accept a URI or file path (Windows- or POSIX-style) and return a URI."
    if not SCHEME_PATTERN.match(str(uri_or_path)):
        # Interpret this as a filepath.
        path = uri_or_path
        uri_str = urlunparse(
            ("file", "localhost", str(Path(path).absolute()), "", "", None)
        )
    else:
        # Interpret this as a URI.
        uri_str = uri_or_path
        parsed = urlparse(uri_str)
        if parsed.netloc == "":
            mutable = list(parsed)
            mutable[1] = "localhost"
            uri_str = urlunparse(mutable)
    return str(uri_str)


def compute_structure_id(structure):
    "Compute HEX digest of MD5 hash of RFC 8785 canonical form of JSON."
    canonical_structure = canonicaljson.encode_canonical_json(structure)

    return hashlib.md5(canonical_structure).hexdigest()
