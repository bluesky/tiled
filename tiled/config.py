"""
This module handles server configuration.

See profiles.py for client configuration.
"""
import collections.abc

from .utils import import_object


def construct_serve_catalogs_args_from_config(config):
    """
    Given parsed configuration, construct arguments for serve_catalogs(...).
    """
    auth_spec = config.get("authenticator")
    # TODO Enable entrypoint as alias for authenticator_class?
    if auth_spec is None:
        authenticator = None
    elif isinstance(auth_spec, str):
        authenticator_class = import_object(auth_spec)
        authenticator = authenticator_class()
    elif isinstance(auth_spec, collections.abc.Mapping):
        ((key, value),) = auth_spec.items()
        authenticator_class = import_object(key)
        authenticator = authenticator_class(**value)
    catalogs = {}
    # TODO Enable entrypoint as alias for pycallable?
    spec_types = {"pyboject", "pycallable", "files"}
    if "catalogs" not in config:
        raise ConfigError("The configuration must include a list of 'catalogs'.")
    for item in config["catalogs"]:
        if "location" not in item:
            raise ConfigError("Each item in 'catalogs' must contain a 'location'.")
        segments = tuple(segment for segment in item["location"].split("/") if segment)
        if "pyobject" in item:
            object_path = item["pyobject"]
            catalog = import_object(object_path)
        elif "pycallable" in item:
            object_path = item["pycallable"]["path"]
            # We only accept keyword args, and we call them 'args',
            # following the example of Ansible configuration, thinking
            # that 'kwargs' is bit too Python-specific for config.
            kwargs = item["pycallable"]["args"]
            catalog_factory = import_object(object_path)
            catalog = catalog_factory(**kwargs)
        elif "files" in item:
            from tiled.catalogs.files import Catalog

            catalog = Catalog.from_directory(**item["files"])
        else:
            raise ConfigError(
                f"Each item in 'catalogs' must contain one of: {spec_types}"
            )
        if segments in catalogs:
            raise ValueError(f"The location {'/'.join(segments)} was specified twice.")
        catalogs[segments] = catalog
        return {"catalogs": catalogs, "authenticator": authenticator}


class ConfigError(ValueError):
    pass
