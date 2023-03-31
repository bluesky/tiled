from pathlib import Path

import yaml

from ..client import from_profile
from ..config import parse_configs
from ..profiles import load_profiles, paths
from ..server.app import build_app_from_config

here = Path(__file__).parent.absolute()


def test_config_imports_custom_python_module():
    "Configs can import from Python modules located in their same directory."
    config_path = here / ".." / ".." / "example_configs" / "custom_export_formats"
    parsed_config = parse_configs(config_path)
    build_app_from_config(parsed_config, source_filepath=config_path)


def test_direct_profile(tmpdir):
    profile_content = {
        "test": {"direct": {"trees": [{"path": "/", "tree": "custom_module:tree"}]}}
    }
    with open(tmpdir / "example.yml", "w") as yaml_file:
        yaml_file.write(yaml.dump(profile_content))
    with open(tmpdir / "custom_module.py", "w") as py_file:
        py_file.write(
            "from tiled.adapters.mapping import MapAdapter; tree = MapAdapter({})"
        )
    profile_dir = Path(tmpdir)
    try:
        paths.append(profile_dir)
        load_profiles.cache_clear()
        from_profile("test")
    finally:
        paths.remove(profile_dir)
