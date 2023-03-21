from pathlib import Path

from ..config import parse_configs
from ..server.app import build_app_from_config

here = Path(__file__).parent.absolute()


def test_config_imports_custom_python_module():
    "Configs can import from Python modules located in their same directory."
    config_path = here / ".." / ".." / "example_configs" / "custom_export_formats"
    parsed_config = parse_configs(config_path)
    build_app_from_config(parsed_config, source_filepath=config_path)
