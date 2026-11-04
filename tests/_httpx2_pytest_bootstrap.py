"""
Early-loading pytest plugin, registered via `pytest.ini`'s `addopts = -p ...`.

respx (used to mock outgoing HTTP calls in several test modules) is written
against `httpx`, which Tiled no longer depends on. pytest loads third-party
plugins registered via the `pytest11` entry point -- including respx's --
before any conftest.py is imported, so aliasing `httpx` to `httpx2` from
tests/conftest.py is too late: respx has already imported the real `httpx`
by then. Loading this module explicitly via `-p` runs before that
entry-point autoloading, so the alias is in place first and respx ends up
patching httpx2's classes, which is what Tiled's HTTP clients actually use.
"""

import httpx2

httpx2.alias_httpx()
