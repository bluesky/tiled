"""
This module handles server configuration.

See profiles.py for client configuration.
"""
from .utils import import_object


def construct_serve_catalogs_args_from_config(config):
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
    catalogs = {}
    # TODO Enable entrypoint to extend aliases?
    catalog_aliases = {"files": "tiled.catalog.files:Catalog.from_directory"}
    if "catalogs" not in config:
        raise ConfigError("The configuration must include a list of 'catalogs'.")
    for item in config["catalogs"]:
        if "path" not in item:
            raise ConfigError("Each item in 'catalogs' must contain a key 'path'.")
        segments = tuple(segment for segment in item["path"].split("/") if segment)
        if "catalog" not in item:
            raise ConfigError("Each item in 'catalogs' must contain a key 'catalog'.")
        catalog_spec = item["catalog"]
        import_path = catalog_aliases.get(catalog_spec, catalog_spec)
        obj = import_object(import_path)
        if "args" in item:
            # Interpret obj as catalog *factory*.
            catalog = obj(**item["args"])
        else:
            # Interpret obj as catalog instance.
            catalog = obj
        if segments in catalogs:
            raise ValueError(f"The path {'/'.join(segments)} was specified twice.")
        catalogs[segments] = catalog
        return {"catalogs": catalogs, "authenticator": authenticator}


class ConfigError(ValueError):
    pass
