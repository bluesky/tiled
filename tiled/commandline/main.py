import enum
from pathlib import Path

import typer
from typing import List, Optional


cli_app = typer.Typer()
serve_app = typer.Typer()
profile_app = typer.Typer()
cli_app.add_typer(serve_app, name="serve")
cli_app.add_typer(profile_app, name="profile")


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


@profile_app.command("paths")
def profile_paths():
    "List the locations that the client will search for profiles (configuration)."
    from ..profiles import paths

    print("\n".join(paths))


@profile_app.command("list")
def profile_list():
    "List the profiles (client-side configuration) found and the files they were read from."
    from ..profiles import discover_profiles

    profiles = discover_profiles()
    if not profiles:
        typer.echo("No profiles found.")
        return
    max_len = max(len(name) for name in profiles)
    PADDING = 4

    print(
        "\n".join(
            f"{name:<{max_len + PADDING}}{filepath}"
            for name, (filepath, _) in profiles.items()
        )
    )


@profile_app.command("show")
def profile_show(profile_name: str):
    "Show the content of a profile."
    import yaml
    import sys

    from ..profiles import discover_profiles

    profiles = discover_profiles()
    try:
        filepath, content = profiles[profile_name]
    except KeyError:
        typer.echo(f"The profile {profile_name!r} could not be found.")
        raise typer.Abort()
    print(f"Source: {filepath}", file=sys.stderr)
    print("--", file=sys.stderr)
    print(yaml.dump(content), file=sys.stdout)


@serve_app.command("directory")
def serve_directory(
    directory: str,
):
    "Serve a Catalog instance from a directory of files."
    from ..catalogs.files import Catalog
    from ..server.app import serve_catalog

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
    from ..server.app import serve_catalog
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
    config: Path,
    format: ConfigFormats = typer.Option(None),
):
    if config.is_file():
        filepaths = [config]
    elif config.is_dir():
        filepaths = list(config.iterdir())
    elif not config.exists():
        typer.echo(f"The config path {config!s} doesn't exist.")
        raise typer.Abort()
    else:
        assert False, "It should be impossible to reach this line."

    from ..utils import infer_config_format, parse

    parsed_configs = {}
    # The sorting here is just to make this deterministic.
    # There is *not* any sorting-based precedence applied.
    for filepath in sorted(filepaths):
        # Ignore hidden files.
        if filepath.parts[-1].startswith("."):
            continue
        format_ = format or infer_config_format(filepath)
        with open(filepath) as file:
            parsed_configs[filepath] = parse(file, format=format_)

    from ..config import construct_serve_catalogs_kwargs, merge

    merged_config = merge(parsed_configs)
    kwargs = construct_serve_catalogs_kwargs(merged_config)

    from ..server.app import serve_catalogs

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
