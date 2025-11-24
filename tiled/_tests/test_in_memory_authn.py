from pathlib import Path

import yaml

from tiled._tests.utils import enter_username_password
from tiled.client import Context, from_context
from tiled.server.app import build_app_from_config

here = Path(__file__).parent.absolute()


def test_good_path():
    """Test authn database defaults to in-memory catalog"""
    with open(here / "test_configs" / "config_in_memory_authn.yml") as config_file:
        config = yaml.load(config_file, Loader=yaml.BaseLoader)

    app = build_app_from_config(config)
    context = Context.from_app(app)

    with enter_username_password("alice", "PASSWORD"):
        client = from_context(context, remember_me=False)

    client.logout()
    context.close()

    assert True
