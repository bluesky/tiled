from functools import lru_cache

import typer
from typing import List
import uvicorn

from ..server.main import api, get_settings
from ..utils import import_object


app = typer.Typer()
serve_app = typer.Typer()
app.add_typer(serve_app, name="serve")


@serve_app.command("directory")
def directory(
    directory: str,
    glob: List[str] = typer.Option(None),
    mimetype: List[str] = typer.Option(None),
):
    "Serve a Catalog instance from a directory of files."
    import os

    if not os.path.isdir(directory):
        raise ValueError(f"{directory} is not a directory")

    from tiled.catalogs.directory import Catalog

    @lru_cache(1)
    def override_settings():
        settings = get_settings()
        settings.catalog = Catalog.from_directory(directory)
        return settings

    api.dependency_overrides[get_settings] = override_settings
    uvicorn.run(api)


@serve_app.command("instance")
def instance(
    instance: str,
    glob: List[str] = typer.Option(None),
    mimetype: List[str] = typer.Option(None),
):
    "Serve a Catalog instance from a Python module."

    @lru_cache(1)
    def override_settings():
        settings = get_settings()
        settings.catalog_path = import_object(instance)
        return settings

    api.dependency_overrides[get_settings] = override_settings
    uvicorn.run(api)


@serve_app.command("factory")
def factory(
    factory: str,
    arg: List[str] = typer.Option(None),
    glob: List[str] = typer.Option(None),
    mimetype: List[str] = typer.Option(None),
):
    "Serve a Catalog instance from a callable in a Python module."

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
    instance = import_object(factory)(**kwargs)

    @lru_cache(1)
    def override_settings():
        settings = get_settings()
        settings.catalog = instance
        return settings

    api.dependency_overrides[get_settings] = override_settings
    uvicorn.run(api)


def main():
    app()


if __name__ == "__main__":
    main()
