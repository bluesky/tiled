"""
This module handles server configuration.

See profiles.py for client configuration.
"""
from collections import defaultdict
import copy
from functools import lru_cache
import os
from pathlib import Path

import jsonschema

from .utils import import_object, parse, prepend_to_sys_path
from .media_type_registration import (
    serialization_registry as default_serialization_registry,
    compression_registry as default_compression_registry,
)
from .query_registration import query_registry as default_query_registry


@lru_cache(maxsize=1)
def schema():
    "Load the schema for service-side configuration."
    import yaml

    here = Path(__file__).parent.absolute()
    schema_path = here / "schemas" / "service_configuration.yml"
    with open(schema_path, "r") as file:
        return yaml.safe_load(file)


def construct_serve_tree_kwargs(
    config,
    *,
    source_filepath=None,
    query_registry=None,
    compression_registry=None,
    serialization_registry=None,
):
    """
    Given parsed configuration, construct arguments for serve_tree(...).

    The parameters query_registry, compression_registry, and
    serialization_registry are used by the tests to inject separate registry
    instances. By default, the singleton global instances of these registries
    and used (and modified).
    """
    config = copy.deepcopy(config)  # Avoid mutating input.
    if query_registry is None:
        query_registry = default_query_registry
    if serialization_registry is None:
        serialization_registry = default_serialization_registry
    if compression_registry is None:
        compression_registry = default_compression_registry
    sys_path_additions = []
    if source_filepath:
        if os.path.isdir(source_filepath):
            directory = source_filepath
        else:
            directory = os.path.dirname(source_filepath)
        sys_path_additions.append(directory)
    with prepend_to_sys_path(*sys_path_additions):
        auth_spec = config.get("authentication", {}) or {}
        root_access_control = config.get("access_control", {}) or {}
        auth_aliases = {}
        # TODO Enable entrypoint as alias for authenticator_class?
        if auth_spec.get("authenticator") is not None:
            import_path = auth_aliases.get(
                auth_spec["authenticator"], auth_spec["authenticator"]
            )
            authenticator_class = import_object(import_path, accept_live_object=True)
            authenticator = authenticator_class(**auth_spec.get("args", {}))
            auth_spec["authenticator"] = authenticator
        if root_access_control.get("access_policy") is not None:
            root_policy_import_path = root_access_control["access_policy"]
            root_policy_class = import_object(
                root_policy_import_path, accept_live_object=True
            )
            root_access_policy = root_policy_class(
                **root_access_control.get("args", {})
            )
        else:
            root_access_policy = None
        # TODO Enable entrypoint to extend aliases?
        tree_aliases = {"files": "tiled.trees.files:Tree.from_directory"}
        trees = {}
        for item in config.get("trees", []):
            access_control = item.get("access_control", {}) or {}
            if access_control.get("access_policy") is not None:
                policy_import_path = access_control["access_policy"]
                policy_class = import_object(
                    policy_import_path, accept_live_object=True
                )
                access_policy = policy_class(**access_control.get("args", {}))
            else:
                access_policy = None
            segments = tuple(segment for segment in item["path"].split("/") if segment)
            tree_spec = item["tree"]
            import_path = tree_aliases.get(tree_spec, tree_spec)
            obj = import_object(import_path, accept_live_object=True)
            if ("args" in item) or (access_policy is not None):
                if not callable(obj):
                    raise ValueError(
                        f"Object imported from {import_path} cannot take args. "
                        "It is not callable."
                    )
                # Interpret obj as tree *factory*.
                if access_policy is not None:
                    if "args" not in item:
                        item["args"] = {}
                    item["args"]["access_policy"] = access_policy
                tree = obj(**item["args"])
            else:
                # Interpret obj as tree instance.
                tree = obj
            if segments in trees:
                raise ValueError(f"The path {'/'.join(segments)} was specified twice.")
            trees[segments] = tree
        if not len(trees):
            raise ValueError("Configuration contains no trees")
        if (len(trees) == 1) and () in trees:
            # There is one tree to be deployed at '/'.
            root_tree = tree
        else:
            # There are one or more tree(s) to be served at
            # sub-paths. Merged them into one root in-memory Tree.
            from .trees.in_memory import Tree

            mapping = {}
            include_routers = []
            for segments, tree in trees.items():
                inner_mapping = mapping
                for segment in segments[:-1]:
                    if segment in inner_mapping:
                        inner_mapping = inner_mapping[segment]
                    else:
                        inner_mapping = inner_mapping[segment] = {}
                inner_mapping[segments[-1]] = tree
                routers = getattr(tree, "include_routers", [])
                for router in routers:
                    if router not in include_routers:
                        include_routers.append(router)
            root_tree = Tree(mapping, access_policy=root_access_policy)
            root_tree.include_routers.extend(include_routers)
        server_settings = {}
        server_settings["allow_origins"] = config.get("allow_origins")
        server_settings["object_cache"] = config.get("object_cache", {})
        for structure_family, values in config.get("media_types", {}).items():
            for media_type, import_path in values.items():
                serializer = import_object(import_path, accept_live_object=True)
                serialization_registry.register(
                    structure_family, media_type, serializer
                )
        for ext, media_type in config.get("file_extensions", {}).items():
            serialization_registry.register_alias(ext, media_type)
    # TODO Make compression_registry extensible via configuration.
    return {
        "tree": root_tree,
        "authentication": auth_spec,
        "server_settings": server_settings,
        "query_registry": query_registry,
        "serialization_registry": serialization_registry,
        "compression_registry": compression_registry,
    }


def merge(configs):
    merged = {"trees": []}

    # These variables are used to produce error messages that point
    # to the relevant config file(s).
    authentication_config_source = None
    access_control_config_source = None
    uvicorn_config_source = None
    object_cache_config_source = None
    allow_origins = []
    media_types = defaultdict(dict)
    file_extensions = {}
    paths = {}  # map each item's path to config file that specified it

    for filepath, config in configs.items():
        for structure_family, values in config.get("media_types", {}).items():
            media_types[structure_family].update(values)
        file_extensions.update(config.get("file_extensions", {}))
        allow_origins.extend(config.get("allow_origins", []))
        if "access_control" in config:
            if "access_control" in merged:
                raise ConfigError(
                    "access_control can only be specified in one file. "
                    f"It was found in both {access_control_config_source} and "
                    f"{filepath}"
                )
            access_control_config_source = filepath
            merged["access_control"] = config["access_control"]
        if "authentication" in config:
            if "authentication" in merged:
                raise ConfigError(
                    "authentication can only be specified in one file. "
                    f"It was found in both {authentication_config_source} and "
                    f"{filepath}"
                )
            authentication_config_source = filepath
            merged["authentication"] = config["authentication"]
        if "uvicorn" in config:
            if "uvicorn" in merged:
                raise ConfigError(
                    "uvicorn can only be specified in one file. "
                    f"It was found in both {uvicorn_config_source} and "
                    f"{filepath}"
                )
            uvicorn_config_source = filepath
            merged["uvicorn"] = config["uvicorn"]
        if "object_cache" in config:
            if "object_cache" in merged:
                raise ConfigError(
                    "object_cache can only be specified in one file. "
                    f"It was found in both {object_cache_config_source} and "
                    f"{filepath}"
                )
            object_cache_config_source = filepath
            merged["object_cache"] = config["object_cache"]
        for item in config.get("trees", []):
            if item["path"] in paths:
                msg = "A given path may be only be specified once."
                "The path {item['path']} was found twice in "
                if filepath == paths[item["path"]]:
                    msg += f"{filepath}."
                else:
                    msg += f"{filepath} and {paths[item['path']]}."
                raise ConfigError(msg)
            paths[item["path"]] = filepath
            merged["trees"].append(item)
    merged["media_types"] = dict(media_types)  # convert from defaultdict
    merged["file_extensions"] = file_extensions
    merged["allow_origins"] = allow_origins
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
            config = parse(file)
            try:
                jsonschema.validate(instance=config, schema=schema())
            except jsonschema.ValidationError as err:
                msg = err.args[0]
                raise ConfigError(
                    f"ValidationError while parsing configuration file {filepath}: {msg}"
                ) from err
            parsed_configs[filepath] = config

    merged_config = merge(parsed_configs)
    return merged_config


def direct_access(config, source_filepath=None):
    """
    Return the server-side Tree object defined by a configuration.

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
                "trees":
                    [
                        "path": "/",
                        "tree": "tiled.files.Tree.from_files",
                        "args": {"diretory": "path/to/files"}
                    ]
            }
        )
    """
    if isinstance(config, (str, Path)):
        parsed_config = parse_configs(config)
    else:
        parsed_config = config
    kwargs = construct_serve_tree_kwargs(parsed_config, source_filepath=source_filepath)
    return kwargs["tree"]


def direct_access_from_profile(name):
    """
    Return the server-side Tree object from a profile.

    Some profiles are purely client side, providing an address like

    uri: ...

    Others have the service-side configuration inline like:

    direct:
      - path: /
        tree: ...

    This function only works on the latter kind. It returns the
    service-side Tree instance directly, not wrapped in a client.
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
