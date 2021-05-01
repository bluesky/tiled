import typer
from typing import List, Optional


cli_app = typer.Typer()
serve_app = typer.Typer()
profiles_app = typer.Typer()
cli_app.add_typer(serve_app, name="serve")
cli_app.add_typer(profiles_app, name="profiles")


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


@profiles_app.command("paths")
def profiles_paths():
    "List the locations that the client will search for profiles (configuration)."
    from tiled.client.profiles import paths

    print("\n".join(paths))


@profiles_app.command("list")
def profiles_list():
    "List the profiles (client-side configuration) found and the files they were read from."
    from tiled.client.profiles import discover_profiles

    profiles = discover_profiles()
    if not profiles:
        return
    max_len = max(len(name) for name in profiles)
    PADDING = 4

    print(
        "\n".join(
            f"{name:<{max_len + PADDING}}{filepath}"
            for name, (filepath, _) in profiles.items()
        )
    )


@serve_app.command("directory")
def serve_directory(
    directory: str,
):
    "Serve a Catalog instance from a directory of files."
    import os
    import uvicorn

    from ..server.main import serve_catalog

    if not os.path.isdir(directory):
        raise ValueError(f"{directory} is not a directory")

    from tiled.catalogs.files import Catalog

    catalog = Catalog.from_directory(directory)
    web_app = serve_catalog(catalog)
    uvicorn.run(web_app)


@serve_app.command("pyobject")
def serve_pyobject(
    object_path: str,  # e.g. "package_name.module_name:object_name"
    glob: List[str] = typer.Option(None),
    mimetype: List[str] = typer.Option(None),
):
    "Serve a Catalog instance from a Python module."
    import uvicorn

    from ..server.main import serve_catalog
    from ..utils import import_object

    # Import eagerly so any errors here get raised
    # before server startup.
    catalog = import_object(object_path)

    web_app = serve_catalog(catalog)
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
