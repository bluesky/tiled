import enum

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
    from ..client.cache import download
    from ..client.catalog import Catalog

    catalog = Catalog.from_uri(catalog_uri)
    download(catalog, path=path, available_bytes=available_bytes)


@profiles_app.command("paths")
def profiles_paths():
    "List the locations that the client will search for profiles (configuration)."
    from ..profiles import paths

    print("\n".join(paths))


@profiles_app.command("list")
def profiles_list():
    "List the profiles (client-side configuration) found and the files they were read from."
    from ..profiles import discover_profiles

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
    from ..catalogs.files import Catalog
    from ..server.main import serve_catalog

    catalog = Catalog.from_directory(directory)
    web_app = serve_catalog(catalog)

    import uvicorn

    uvicorn.run(web_app)


@serve_app.command("pyobject")
def serve_pyobject(
    object_path: str,  # e.g. "package_name.module_name:object_name"
    glob: List[str] = typer.Option(None),
    mimetype: List[str] = typer.Option(None),
):
    "Serve a Catalog instance from a Python module."
    from ..server.main import serve_catalog
    from ..utils import import_object

    catalog = import_object(object_path)

    web_app = serve_catalog(catalog)

    import uvicorn

    uvicorn.run(web_app)


class ConfigFormats(str, enum.Enum):
    yaml = "yaml"
    toml = "toml"
    json = "json"


@serve_app.command("config")
def serve_config(
    config: typer.FileText,
    format: ConfigFormats = typer.Option(None),
):
    from ..config import construct_serve_catalogs_args_from_config
    from ..utils import infer_config_format, parse
    from ..server.main import serve_catalogs

    if format is None:
        if config.name == "<stdin>":
            # We just have to guess because we have no file extension to work with.
            format = "yaml"
        else:
            format = infer_config_format(config.name)
    parsed_content = parse(config)
    kwargs = construct_serve_catalogs_args_from_config(parsed_content)

    web_app = serve_catalogs(**kwargs)

    import uvicorn

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
