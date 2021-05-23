"""
This module handles server configuration.

See profiles.py for client configuration.
"""
import contextlib
import os
from pathlib import Path

from .utils import import_object, parse


def construct_serve_catalog_kwargs(config, source_filepath=None):
    """
    Given parsed configuration, construct arguments for serve_catalog(...).
    """
    auth_spec = config.get("authentication", {})
    allow_anonymous_access = bool(auth_spec.get("allow_anonymous_access"))
    auth_aliases = {}
    # TODO Enable entrypoint as alias for authenticator_class?
    if auth_spec.get("authenticator") is None:
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
                if os.path.isdir(source_filepath):
                    directory = source_filepath
                else:
                    directory = os.path.dirname(source_filepath)
                sys_path_additions.append(directory)
            with _prepend_to_sys_path(sys_path_additions):
                catalog = obj(**item["args"])
        else:
            # Interpret obj as catalog instance.
            catalog = obj
        if segments in catalogs:
            raise ValueError(f"The path {'/'.join(segments)} was specified twice.")
        catalogs[segments] = catalog
    if (len(catalogs) == 1) and () in catalogs:
        # There is one catalog to be deployed at '/'.
        root_catalog = catalog
    else:
        # There are one or more catalog(s) to be served at
        # sub-paths. Merged them into one root in-memory Catalog.
        from .catalogs.in_memory import Catalog

        mapping = {}
        include_routers = []
        for segments, catalog in catalogs.items():
            inner_mapping = mapping
            for segment in segments[:-1]:
                if segment in inner_mapping:
                    inner_mapping = inner_mapping[segment]
                else:
                    inner_mapping = inner_mapping[segment] = {}
            inner_mapping[segments[-1]] = catalog
            routers = getattr(catalog, "include_routers", [])
            for router in routers:
                if router not in include_routers:
                    include_routers.append(router)
        root_catalog = Catalog(mapping)
        root_catalog.include_routers.extend(include_routers)
    return {
        "catalog": root_catalog,
        "authenticator": authenticator,
        "allow_anonymous_access": allow_anonymous_access,
        "secret_keys": auth_spec.get("secret_keys"),
    }


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


def parse_configs(config_path):
    """
    Parse configuration file or directory of configuration files.

    If a directory is given it is expected to contain only valid
    configuration files, except for the following which are ignored:

    * Hidden files or directories (starting with .)
    * Python scripts (ending in .py)
    * The __pycache__ directory
    """
    if isinstance(config_path, str):
        config_path = Path(config_path)
    if config_path.is_file():
        filepaths = [config_path]
    elif config_path.is_dir():
        filepaths = list(config_path.iterdir())
    elif not config_path.exists():
        raise ValueError(f"The config path {config_path!s} doesn't exist.")
    else:
        assert False, "It should be impossible to reach this line."

    parsed_configs = {}
    # The sorting here is just to make the order of the results deterministic.
    # There is *not* any sorting-based precedence applied.
    for filepath in sorted(filepaths):
        # Ignore hidden files and .py files.
        if (
            filepath.parts[-1].startswith(".")
            or filepath.suffix == ".py"
            or filepath.parts[-1] == "__pycache__"
        ):
            continue
        with open(filepath) as file:
            parsed_configs[filepath] = parse(file)

    merged_config = merge(parsed_configs)
    return merged_config


def direct_access(config, source_filepath=None):
    """
    Return the server-side Catalog object defined by a configuration.

    Parameters
    ----------
    config : str or dict
        May be:

        * Path to config file
        * Path to directory of config files
        * Dict of config

    Examples
    --------

    From config file:

    >>> from_config("path/to/file.yml")

    From directory of config files:

    >>> from_config("path/to/directory")

    From configuration given directly, as dict:

    >>> from_config(
            {
                "catalogs":
                    [
                        "path": "/",
                        "catalog": "tiled.files.Catalog.from_files",
                        "args": {"diretory": "path/to/files"}
                    ]
            }
        )
    """
    if isinstance(config, (str, Path)):
        parsed_config = parse_configs(config)
    else:
        parsed_config = config
    return construct_serve_catalog_kwargs(parsed_config, source_filepath)["catalog"]


def direct_access_from_profile(name):
    """
    Return the server-side Catalog object from a profile.

    Some profiles are purely client side, providing an address like

    uri: ...

    Others have the service-side configuration inline like:

    direct:
      - path: /
        catalog: ...

    This function only works on the latter kind. It returns the
    service-side Catalog instance directly, not wrapped in a client.
    """

    from .profiles import load_profiles, paths, ProfileNotFound

    profiles = load_profiles()
    try:
        filepath, profile_content = profiles[name]
    except KeyError as err:
        raise ProfileNotFound(
            f"Profile {name!r} not found. Found profiles {list(profiles)} "
            f"from directories {paths}."
        ) from err
    if "direct" not in profile_content:
        raise ValueError(
            "The function direct_access_from_profile only works on "
            "profiles with a 'direct:' section that contain the "
            "service-side configuration inline."
        )
    return direct_access(profile_content["direct"], source_filepath=filepath)


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
