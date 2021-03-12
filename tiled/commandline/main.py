from functools import lru_cache
import os

import typer
from typing import List
import uvicorn

from ..server.main import api, get_settings


app = typer.Typer()


@app.command("serve")
def serve(
    target: str,
    glob: List[str] = typer.Option(None),
    mimetype: List[str] = typer.Option(None),
):
    if ":" in target:

        @lru_cache(1)
        def override_settings():
            settings = get_settings()
            settings.catalog_object_path = target
            return settings

    elif os.path.isdir(target):
        from tiled.catalogs.directory import Catalog

        @lru_cache(1)
        def override_settings():
            settings = get_settings()
            settings.catalog_object = Catalog.from_directory(target)
            return settings

    else:
        raise ValueError("Expected path/to/directory or package.module:object")

    api.dependency_overrides[get_settings] = override_settings
    uvicorn.run(api)


def main():
    app()


if __name__ == "__main__":
    main()
