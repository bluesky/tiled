from functools import lru_cache

import typer
from typing import List, Optional
import uvicorn

from ..server.main import app as web_app, get_settings
from ..utils import import_object


cli_app = typer.Typer()
serve_app = typer.Typer()
cli_app.add_typer(serve_app, name="serve")


@cli_app.command("download")
def download(
    catalog_uri: str,
    path: str,
    available_bytes: Optional[int] = None,
):
    from tiled.client.cache import download
    from tiled.client.catalog import Catalog

    catalog = Catalog.from_uri(catalog_uri)
    download(catalog, path=path, available_bytes=available_bytes)


@serve_app.command("directory")
def directory(
    directory: str,
):
    "Serve a Catalog instance from a directory of files."
    import os

    if not os.path.isdir(directory):
        raise ValueError(f"{directory} is not a directory")

    from tiled.catalogs.files import Catalog

    @lru_cache(1)
    def override_settings():
        settings = get_settings()
        settings.catalog = Catalog.from_directory(directory)
        return settings

    web_app.dependency_overrides[get_settings] = override_settings
    uvicorn.run(web_app)


@serve_app.command("pyobject")
def pyobject(
    object_path: str,  # e.g. "package_name.module_name:object_name"
    glob: List[str] = typer.Option(None),
    mimetype: List[str] = typer.Option(None),
):
    "Serve a Catalog instance from a Python module."

    # Import eagerly so any errors here get raised
    # before server startup.
    catalog = import_object(object_path)

    @lru_cache(1)
    def override_settings():
        settings = get_settings()
        settings.catalog = catalog
        return settings

    web_app.dependency_overrides[get_settings] = override_settings
    uvicorn.run(web_app)


def _parse_kwargs(arg):
    # Parse
    # --arg a="x" --arg b=1
    # into
    # {"a": "x", "b": 1}
    import ast

    kwargs = {}
    for full_str in arg:
        keyword, value_str = full_str.split("=", 1)
        value = ast.literal_eval(value_str)
        kwargs[keyword] = value
    return kwargs


main = cli_app
if __name__ == "__main__":
    main()
