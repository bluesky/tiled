"""
This tests the feature that is exercised by the --scalable CLI option.
"""
from pathlib import Path

import pytest

from tiled.server.settings import Settings

from ..config import parse_configs
from ..server.app import UnscalableConfig, build_app

here = Path(__file__).parent.absolute()


@pytest.mark.parametrize(
    "filename, scalable",
    [
        ("config_with_api_key.yml", True),
        ("config_with_secret_keys.yml", True),
        ("config_public_no_authenticator.yml", True),
        ("config_missing_api_key.yml", False),
        ("config_missing_secret_keys.yml", False),
        ("config_missing_secret_keys_public.yml", False),
    ],
)
def test_scalable_config(filename, scalable):
    config_path = here / "test_configs" / filename
    settings: Settings = parse_configs(config_path)
    if scalable:
        build_app(scalable=True, server_settings=settings)
        build_app(scalable=False, server_settings=settings)
    else:
        with pytest.raises(UnscalableConfig):
            build_app(scalable=True, server_settings=settings)
        build_app(scalable=False, server_settings=settings)
