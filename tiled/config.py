"""
This module handles server configuration.

See profiles.py for client configuration.
"""
from pathlib import Path

from pydantic import TypeAdapter
from yaml import safe_load

from tiled.server.settings import Settings


def parse_configs(config_path: Path) -> Settings:
    """
    Parse configuration file.
    """
    if config_path.is_dir():
        raise ValueError
    if not config_path.exists():
        raise ValueError(f"The config path {config_path!s} doesn't exist.")

    with open(config_path) as file:
        config = safe_load(file)
        return TypeAdapter(Settings).validate_python(config)
