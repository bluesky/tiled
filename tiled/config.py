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
    # TODO Enable entrypoint aliases?
    auth_spec = config.get("authenticator")
    if isinstance(auth_spec, str):
        authenticator_class = import_object(auth_spec)
        authenticator = authenticator_class()
    elif isinstance(auth_spec, collections.abc.Mapping):
        ((key, value),) = auth_spec.items()
        authenticator_class = import_object(key)
        authenticator = authenticator_class(**value)
    catalogs = {}
    for item in config["catalogs"]:
        segments = tuple(segment for segment in item["path"].split("/") if segment)
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
        # TODO Enable entrypoint aliases?
        else:
            raise ValueError
        if segments in catalogs:
            raise ValueError(f"The path {'/'.join(segments)} was specified twice.")
        catalogs[segments] = catalog
        return {"catalogs": catalogs, "authenticator": authenticator}
