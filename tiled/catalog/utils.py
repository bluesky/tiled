import re
import sys
from pathlib import Path

import httpx

SCHEME_PATTERN = re.compile(r"^[a-z0-9]+:\/\/.*$")


def safe_path(uri):
    raw_path = httpx.URL(uri).path
    if sys.platform == "win32" and raw_path[0] == "/":
        path = raw_path[1:]
    else:
        path = raw_path
    return Path(path)


def ensure_uri(uri):
    if not SCHEME_PATTERN.match(uri):
        uri = "file://localhost/" + str(Path(uri).absolute())
    uri = httpx.URL(uri)
    if uri.scheme == "file":
        uri = uri.copy_with(host="localhost")
    return uri
