from pathlib import Path

import numpy
import yaml

from tiled._tests.utils import enter_username_password
from tiled.client import Context, from_context
from tiled.server.app import build_app_from_config

CONFIG_NAME = "toy_authentication.yml"
CATALOG_STORAGE = "data/"


def main():
    file_directory = Path(__file__).resolve().parent
    config_directory = file_directory.parent
    Path(file_directory, CATALOG_STORAGE).mkdir()

    with open(Path(config_directory, CONFIG_NAME)) as config_file:
        config = yaml.load(config_file, Loader=yaml.BaseLoader)
    app = build_app_from_config(config)
    context = Context.from_app(app)
    with enter_username_password("admin", "admin"):
        client = from_context(context, remember_me=False)
    for n in ["A", "B", "C", "D"]:
        client.write_array(
            key=n, array=10 * numpy.ones((10, 10)), access_tags=[f"data_{n}"]
        )
    client.logout()
    context.close()


if __name__ == "__main__":
    main()
