"""
This module handles server configuration.

See profiles.py for client configuration.
"""
import contextlib
import os

from .utils import import_object


def construct_serve_catalogs_kwargs(config, source_filepath=None):
    """
    Given parsed configuration, construct arguments for serve_catalogs(...).
    """
    auth_spec = config.get("authentication")
    auth_aliases = {}
    # TODO Enable entrypoint as alias for authenticator_class?
    if auth_spec is None:
        authenticator = None
    else:
        import_path = auth_aliases.get(
            auth_spec["authenticator"], auth_spec["authenticator"]
        )
        authenticator_class = import_object(import_path)
        authenticator = authenticator_class(**auth_spec.get("args", {}))
    # TODO Enable entrypoint to extend aliases?
    catalog_aliases = {"files": "tiled.catalogs.files:Catalog.from_directory"}
    catalogs = {}
    for item in config.get("catalogs", []):
        if "path" not in item:
            raise ConfigError("Each item in 'catalogs' must contain a key 'path'.")
        segments = tuple(segment for segment in item["path"].split("/") if segment)
        if "catalog" not in item:
            raise ConfigError("Each item in 'catalogs' must contain a key 'catalog'.")
        catalog_spec = item["catalog"]
        import_path = catalog_aliases.get(catalog_spec, catalog_spec)
        obj = import_object(import_path)
        if "args" in item:
            if not callable(obj):
                raise ValueError(
                    f"Object imported from {import_path} cannot take args. "
                    "It is not callable."
                )
            # Interpret obj as catalog *factory*.
            sys_path_additions = []
            if source_filepath:
                sys_path_additions.append(os.path.dirname(source_filepath))
            with _prepend_to_sys_path(sys_path_additions):
                catalog = obj(**item["args"])
        else:
            # Interpret obj as catalog instance.
            catalog = obj
        if segments in catalogs:
            raise ValueError(f"The path {'/'.join(segments)} was specified twice.")
        catalogs[segments] = catalog
    return {"catalogs": catalogs, "authenticator": authenticator}


def merge(configs):
    merged = {"catalogs": []}

    # These two variables are used to produce error messages that point
    # to the relevant config file(s).
    authentication_config_source = None
    paths = {}  # map each item's path to config file that specified it

    for filepath, config in configs.items():
        if "authentication" in config:
            if "authentication" in merged:
                raise ConfigError(
                    "authentication can only be specified in one file. "
                    f"It was found in both {authentication_config_source} and "
                    f"{filepath}"
                )
            authentication_config_source = filepath
            merged["authentication"] = config["authentication"]
        for item in config.get("catalogs", []):
            if item["path"] in paths:
                msg = "A given path may be only be specified once."
                "The path {item['path']} was found twice in "
                if filepath == paths[item["path"]]:
                    msg += f"{filepath}."
                else:
                    msg += f"{filepath} and {paths[item['path']]}."
                raise ConfigError(msg)
            paths[item["path"]] = filepath
            merged["catalogs"].append(item)
        merged["catalogs"]
    return merged


class ConfigError(ValueError):
    pass


@contextlib.contextmanager
def _prepend_to_sys_path(path):
    "Temporarily prepend items to sys.path."
    import sys

    for item in reversed(path):
        sys.path.insert(0, item)
    try:
        yield
    finally:
        for item in path:
            sys.path.pop(0)
