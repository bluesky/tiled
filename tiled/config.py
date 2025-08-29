"""
This module handles server configuration.

See profiles.py for client configuration.
"""

import copy
import os
import warnings
from collections import defaultdict
from datetime import timedelta
from functools import cache
from pathlib import Path
from typing import Any, Union

import jsonschema

from .adapters.mapping import MapAdapter
from .catalog import from_uri, in_memory
from .media_type_registration import (
    default_compression_registry,
    default_deserialization_registry,
    default_serialization_registry,
)
from .query_registration import default_query_registry
from .server.settings import Settings
from .structures.core import Spec
from .utils import import_object, parse, prepend_to_sys_path
from .validation_registration import default_validation_registry


@cache
def schema():
    "Load the schema for service-side configuration."
    import yaml

    here = Path(__file__).parent.absolute()
    schema_path = here / "config_schemas" / "service_configuration.yml"
    with open(schema_path, "r") as file:
        return yaml.safe_load(file)


def construct_build_app_kwargs(
    config, *, source_filepath: Union[Path, str, None] = None
):
    """
    Given parsed configuration, construct arguments for build_app(...).

    The singleton global instances of the registries are used (and modified).
    """
    config = copy.deepcopy(config)  # Avoid mutating input.
    startup_tasks = []
    shutdown_tasks = []
    background_tasks = []

    sys_path_additions = []
    if source_filepath:
        if os.path.isdir(source_filepath):
            directory = source_filepath
        else:
            directory = os.path.dirname(source_filepath)
        sys_path_additions.append(directory)
    with prepend_to_sys_path(*sys_path_additions):
        # Process auth settings
        auth_spec = config.get("authentication", {}) or {}
        for age in ["refresh_token_max_age", "session_max_age", "access_token_max_age"]:
            if age in auth_spec:
                auth_spec[age] = timedelta(seconds=auth_spec[age])
        access_control = config.get("access_control", {}) or {}
        providers = list(auth_spec.get("providers", []))
        provider_names = [p["provider"] for p in providers]
        if len(set(provider_names)) != len(provider_names):
            raise ValueError(
                "The names given for 'provider' must be unique. "
                f"Found duplicates in {providers}"
            )
        for i, authenticator in enumerate(providers):
            authenticator_class = import_object(
                authenticator["authenticator"], accept_live_object=True
            )
            authenticator = authenticator_class(**authenticator.get("args", {}))
            # Replace "package.module:Object" with live instance.
            auth_spec["providers"][i]["authenticator"] = authenticator
        if access_control.get("access_policy") is not None:
            access_policy_import_path = access_control["access_policy"]
            access_policy_class = import_object(
                access_policy_import_path, accept_live_object=True
            )
            access_policy = access_policy_class(**access_control.get("args", {}))
        else:
            access_policy = None
        # TODO Enable entrypoint to extend aliases?

        # Process server settings
        server_settings = {}
        if root_path := config.get("root_path", ""):
            server_settings["root_path"] = root_path
        server_settings["allow_origins"] = config.get("allow_origins")
        server_settings["response_bytesize_limit"] = config.get(
            "response_bytesize_limit"
        )
        server_settings["exact_count_limit"] = config.get("exact_count_limit")
        server_settings["database"] = config.get("database", {})
        server_settings["reject_undeclared_specs"] = config.get(
            "reject_undeclared_specs"
        )
        server_settings["expose_raw_assets"] = config.get("expose_raw_assets")
        server_settings["metrics"] = config.get("metrics", {})

        # Process trees
        tree_aliases = {
            "catalog": "tiled.catalog:from_uri",
        }
        trees = {}
        for item in config.get("trees", []):
            segments = tuple(segment for segment in item["path"].split("/") if segment)
            tree_spec = item["tree"]
            if isinstance(tree_spec, str) and tree_spec == "files":
                raise Exception(
                    """The way that tiled serves files has changed.

See documentation section "Serve a Directory of Files"."""
                )
            import_path = tree_aliases.get(tree_spec, tree_spec)
            obj = import_object(import_path, accept_live_object=True)
            if ("args" in item) and (not callable(obj)):
                raise ValueError(
                    f"Object imported from {import_path} cannot take args. "
                    "It is not callable."
                )
            if callable(obj):
                # Interpret obj as a tree *factory*.
                args = {}
                args.update(item.get("args", {}))

                # Add other server-related settings falling back to Settings defaults
                # TODO: To be refactroed; these parameters should be in `server_settings`
                if obj is from_uri:
                    default_settings = Settings().model_dump()
                    from_server_settings = {
                        k: config.get(k, default_settings[k])
                        for k in {
                            "catalog_pool_size",
                            "storage_pool_size",
                            "catalog_max_overflow",
                            "storage_max_overflow",
                        }
                    }
                    args.update(from_server_settings)
                if (obj is from_uri) or (obj is in_memory):
                    args.update({"cache_settings": config.get("streaming_cache")})
                tree = obj(**args)
            else:
                # Interpret obj as a tree *instance*.
                tree = obj
            if segments in trees:
                raise ValueError(f"The path {'/'.join(segments)} was specified twice.")
            trees[segments] = tree
            startup_tasks.extend(getattr(tree, "startup_tasks", []))
            shutdown_tasks.extend(getattr(tree, "shutdown_tasks", []))
            background_tasks.extend(getattr(tree, "background_tasks", []))
        if not len(trees):
            raise ValueError("Configuration contains no trees")
        if list(trees) == [()]:
            # Simple case: there is one tree, served at the root path /.
            root_tree = tree
        else:
            # There are one or more tree(s) to be served at
            # sub-paths. Merged them into one root MapAdapter.
            # Map path segments to dicts containing Adapters at that path.
            root_mapping = {}
            index = {(): root_mapping}
            include_routers = []
            for segments, tree in trees.items():
                for i in range(len(segments)):
                    if segments[:i] not in index:
                        mapping = {}
                        index[segments[:i]] = mapping
                        parent = index[segments[: i - 1]]
                        parent[segments[i - 1]] = MapAdapter(mapping)
                index[segments[:-1]][segments[-1]] = tree
                # Collect any custom routers.
                routers = getattr(tree, "include_routers", [])
                for router in routers:
                    if router not in include_routers:
                        include_routers.append(router)
            root_tree = MapAdapter(root_mapping)
            root_tree.include_routers.extend(include_routers)

        # Process other configuration items
        for structure_family, values in config.get("media_types", {}).items():
            for media_type, import_path in values.items():
                serializer = import_object(import_path, accept_live_object=True)
                default_serialization_registry.register(
                    structure_family, media_type, serializer
                )
        for ext, media_type in config.get("file_extensions", {}).items():
            default_serialization_registry.register_alias(ext, media_type)

        for item in config.get("specs", []):
            if "validator" in item:
                validator = import_object(item["validator"])
            else:
                # no-op
                validator = _no_op_validator
            default_validation_registry.register(Spec(item["spec"]), validator)

    # TODO Make compression_registry extensible via configuration.
    return {
        "tree": root_tree,
        "authentication": auth_spec,
        "server_settings": server_settings,
        "query_registry": default_query_registry,
        "serialization_registry": default_serialization_registry,
        "deserialization_registry": default_deserialization_registry,
        "compression_registry": default_compression_registry,
        "validation_registry": default_validation_registry,
        "tasks": {
            "startup": startup_tasks,
            "shutdown": shutdown_tasks,
            "background": background_tasks,
        },
        "access_policy": access_policy,
    }


def merge(configs: dict[Path, dict[str, Any]]) -> dict[str, Any]:
    merged: dict[str, Any] = {"trees": []}

    # These variables are used to produce error messages that point
    # to the relevant config file(s).
    authentication_config_source = None
    access_control_config_source = None
    uvicorn_config_source = None
    metrics_config_source = None
    database_config_source = None
    response_bytesize_limit_config_source = None
    allow_origins = []
    media_types = defaultdict(dict)
    specs = []
    reject_undeclared_specs_source = None
    streaming_cache_source = None
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
            warnings.warn(
                "The object cache has been removed. "
                "The config of the object cache no longer has any effect."
            )
        if "response_bytesize_limit" in config:
            if "response_bytesize_limit" in merged:
                raise ConfigError(
                    "response_bytesize_limit can only be specified in one file. "
                    f"It was found in both {response_bytesize_limit_config_source} and "
                    f"{filepath}"
                )
            response_bytesize_limit_config_source = filepath
            merged["response_bytesize_limit"] = config["response_bytesize_limit"]
        if "metrics" in config:
            if "metrics" in merged:
                raise ConfigError(
                    "metrics can only be specified in one file. "
                    f"It was found in both {metrics_config_source} and "
                    f"{filepath}"
                )
            metrics_config_source = filepath
            merged["metrics"] = config["metrics"]
        if "database" in config:
            if "database" in merged:
                raise ConfigError(
                    "database configuration can only be specified in one file. "
                    f"It was found in both {database_config_source} and "
                    f"{filepath}"
                )
            database_config_source = filepath
            merged["database"] = config["database"]
        if "reject_undeclared_specs" in config:
            if "reject_undeclared_specs" in merged:
                raise ConfigError(
                    "'reject_undeclared_specs' can only be specified in one file. "
                    f"It was found in both {reject_undeclared_specs_source} and "
                    f"{filepath}"
                )
            reject_undeclared_specs_source = filepath
            merged["reject_undeclared_specs"] = config["reject_undeclared_specs"]
        if "streaming_cache" in config:
            if "streaming_cache" in merged:
                raise ConfigError(
                    "'streaming_cache' can only be specified in one file. "
                    f"It was found in both {streaming_cache_source} and "
                    f"{filepath}"
                )
            streaming_cache_source = filepath
            merged["streaming_cache"] = config["streaming_cache"]
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
        specs.extend(config.get("specs", []))
    merged["media_types"] = dict(media_types)  # convert from defaultdict
    merged["file_extensions"] = file_extensions
    merged["allow_origins"] = allow_origins
    merged["specs"] = specs
    return merged


def parse_configs(config_path: Union[str, Path]) -> dict[str, Any]:
    """
    Parse configuration file or directory of configuration files.

    If a directory is given, any files not ending in `.yml` or `.yaml` are
    ignored. Therefore, the directory may also contain a README file and
    supporting Python scripts with custom objects.
    """
    if isinstance(config_path, str):
        config_path = Path(config_path)
    if config_path.is_file():
        filepaths = [config_path]
    elif config_path.is_dir():
        filepaths = [
            fn
            for fn in config_path.iterdir()
            if fn.suffix in (".yml", ".yaml") and fn.is_file()
        ]
    elif not config_path.exists():
        raise ValueError(f"The config path {config_path!s} doesn't exist.")
    else:
        # the path points to something we don't support, eg fifo/block_device/etc
        raise ValueError(
            f"The config path {config_path!s} exists but is not a file or directory."
        )

    parsed_configs: dict[Path, dict[str, Any]] = {}
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


class ConfigError(ValueError):
    pass


def _no_op_validator(*args, **kwargs):
    return None
