import re
import sys
from pathlib import Path
from urllib import parse

import httpx

SCHEME_PATTERN = re.compile(r"^[a-z0-9+]+:\/\/.*$")


def safe_path(uri):
    """
    Acceess the path of a URI and return it as a Path object.

    Ideally we could just do uri.path, but Windows paths confuse
    HTTP URI parsers because of the drive (e.g. C:) and return
    something like /C:/x/y/z with an extraneous leading slash.
    """
    raw_path = httpx.URL(uri).path
    if sys.platform == "win32" and raw_path[0] == "/":
        path = raw_path[1:]
    else:
        path = raw_path
    return Path(path)


def ensure_uri(uri_or_path):
    "Accept a URI or file path (Windows- or POSIX-style) and return a URI."
    if not SCHEME_PATTERN.match(str(uri_or_path)):
        # Interpret this as a filepath.
        path = uri_or_path
        uri_str = parse.urlunparse(
            ("file", "localhost", str(Path(path).absolute()), "", "", None)
        )
    else:
        # Interpret this as a URI.
        uri_str = uri_or_path
    uri = httpx.URL(uri_str)
    # Ensure that, if the scheme is file, it meets the techincal standard for
    # file URIs, like file://localhost/..., not the shorthand file:///...
    if uri.scheme == "file":
        uri = uri.copy_with(host="localhost")
    return uri
