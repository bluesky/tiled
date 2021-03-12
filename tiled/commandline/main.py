from functools import lru_cache

import typer
from typing import List
import uvicorn

from ..server.main import api, get_settings
from ..utils import import_object


app = typer.Typer()
serve_app = typer.Typer()
app.add_typer(serve_app, name="serve")


@serve_app.command("file_list")
def file_list(
    file: List[str],
    reader_for_glob: List[str] = typer.Option(None),
    reader_for_mimetype: List[str] = typer.Option(None),
):
    "Serve a Catalog instance from a directory of files."
    from tiled.catalogs.files import Catalog

    @lru_cache(1)
    def override_settings():
        settings = get_settings()
        settings.catalog = Catalog.from_files(
            *file,
            reader_for_glob={
                k: import_object(v) for k, v in _parse_kwargs(reader_for_glob).items()
            },
            reader_for_mimetype={
                k: import_object(v)
                for k, v in _parse_kwargs(reader_for_mimetype).items()
            },
        )
        return settings

    api.dependency_overrides[get_settings] = override_settings
    uvicorn.run(api)


@serve_app.command("directory")
def directory(
    directory: str,
    reader_for_glob: List[str] = typer.Option(None),
    reader_for_mimetype: List[str] = typer.Option(None),
):
    "Serve a Catalog instance from a directory of files."
    import os

    if not os.path.isdir(directory):
        raise ValueError(f"{directory} is not a directory")

    from tiled.catalogs.files import Catalog

    @lru_cache(1)
    def override_settings():
        settings = get_settings()
        settings.catalog = Catalog.from_directory(
            directory,
            reader_for_glob={
                k: import_object(v) for k, v in _parse_kwargs(reader_for_glob).items()
            },
            reader_for_mimetype={
                k: import_object(v)
                for k, v in _parse_kwargs(reader_for_mimetype).items()
            },
        )
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
        settings.catalog = import_object(instance)
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

    kwargs = {k: import_object(v) for k, v in _parse_kwargs(arg)}
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


if __name__ == "__main__":
    main()
