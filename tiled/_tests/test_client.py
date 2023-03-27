from pathlib import Path

import httpx
import yaml

from ..adapters.mapping import MapAdapter
from ..client import Context, from_context, from_profile
from ..profiles import load_profiles, paths
from ..server.app import build_app
from .utils import fail_with_status_code

tree = MapAdapter({})


def test_configurable_timeout():
    with Context.from_app(build_app(tree), timeout=httpx.Timeout(17)) as context:
        assert context.http_client.timeout.connect == 17
        assert context.http_client.timeout.read == 17


def test_client_version_check():
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)

        # Too-old user agent should generate a 400.
        context.http_client.headers["user-agent"] = "python-tiled/0.1.0a77"
        with fail_with_status_code(400):
            list(client)

        # Gibberish user agent should generate a 400.
        context.http_client.headers["user-agent"] = "python-tiled/gibberish"
        with fail_with_status_code(400):
            list(client)


def test_direct(tmpdir):
    profile_content = {
        "test": {
            "direct": {
                "trees": [
                    {"path": "/", "tree": "tiled.examples.generated_minimal:tree"}
                ]
            }
        }
    }
    with open(tmpdir / "example.yml", "w") as file:
        file.write(yaml.dump(profile_content))
    load_profiles.cache_clear()
    profile_dir = Path(tmpdir)
    try:
        paths.insert(0, profile_dir)
        from_profile("test")
    finally:
        paths.remove(profile_dir)
