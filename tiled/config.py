"""
This module handles server configuration.

See profiles.py for client configuration.
"""

import copy
import os
import warnings
from collections import defaultdict
from datetime import timedelta
from functools import cache, cached_property
from pathlib import Path
from typing import Annotated, Any, Iterator, Optional, Self, TypedDict, Union

# import jsonschema
from pydantic import BaseModel, Field, ImportString, field_validator, model_validator

from tiled.server.protocols import ExternalAuthenticator, InternalAuthenticator
from tiled.type_aliases import AppTask, TaskMap

from .adapters.mapping import MapAdapter
from .media_type_registration import (
    CompressionRegistry,
    SerializationRegistry,
    default_compression_registry,
    default_deserialization_registry,
    default_serialization_registry,
)
from .query_registration import QueryRegistry, default_query_registry
from .utils import import_object, parse, prepend_to_sys_path
from .validation_registration import ValidationRegistry, default_validation_registry

TREE_ALIASES = {"catalog": "tiled.catalog:from_uri"}

def sub_paths(segments: tuple[str, ...]) -> Iterator[tuple[str, ...]]:
    for i in range(len(segments)):
        yield segments[:i]


class TreeSpec(BaseModel):
    tree: ImportString
    path: str
    args: Optional[dict[str, Any]] = None

    @model_validator(mode="after")
    def check_callable(self) -> Self:
        if self.args and not callable(self.tree):
            raise ValueError(f"Tree type '{self.tree.__class__}' is not callable and cannot take args")
        return self

    @cached_property
    def task_map(self) -> TaskMap:
        _, tree = self.tree_entry
        return { # type: ignore - we have to assume trees only have valid tasks
                "background": getattr(tree, "background_tasks", {}),
                "startup": getattr(tree, "startup_tasks", {}),
                "shutdown": getattr(tree, "shuutdown_tasks", {}),
                }

    @cached_property
    def segments(self) -> tuple[str, ...]:
        return tuple(segment for segment in self.path.split('/') if segment)

    @cached_property
    def tree_entry(self) -> tuple[tuple[str, ...], Any]:
        if self.args:
            return (self.segments, self.tree(**self.args))
        return (self.segments, self.tree)


    @field_validator("tree", mode="before")
    @classmethod
    def tree_alias(cls, value: Any) -> Any:
        return TREE_ALIASES.get(value, value)


class AuthenticationProviderSpec(BaseModel):
    provider: str
    authenticator: ImportString
    args: Optional[dict[str, Any]] = None

    def into_auth_entry(self) -> tuple[str, Union[InternalAuthenticator, ExternalAuthenticator]]:
        auth = self.authenticator(**(self.args or {}))
        if not isinstance(auth, (InternalAuthenticator, ExternalAuthenticator)):
            raise ValueError(f"Type {self.authenticator.__class__} is not a known authenticator type")
        return (self.provider, auth)


class TiledAdmin(TypedDict):
    provider: str
    id: str

class Authentication(BaseModel):
    providers: Annotated[list[AuthenticationProviderSpec], Field(default_factory=list)]
    tiled_admins: Optional[list[TiledAdmin]] = None
    secret_keys: Optional[list[str]] = None
    allow_anonymous_access: bool = False
    single_user_api_key: Annotated[Optional[str], Field(pattern="[a-zA-Z0-9]+")] = None
    access_token_max_age: timedelta = timedelta(minutes=15)
    refresh_token_max_age: timedelta = timedelta(days=7)
    session_max_age: Optional[timedelta] = None

    @field_validator("providers", mode="after")
    @classmethod
    def check_unique_names(cls, value: list[AuthenticationProviderSpec]) -> list[AuthenticationProviderSpec]:
        if value is not None:
            if len(value) != len(set(s.provider for s in value)):
                raise ValueError("Authenticator provider names must be unique")
        return value or []

    @cached_property
    def authenticators(self) -> dict[str, Union[InternalAuthenticator, ExternalAuthenticator]]:
        return dict(auth.into_auth_entry() for auth in self.providers)


class Database(BaseModel):
    uri: str
    init_if_not_exists: bool = False
    pool_pre_ping: bool = False
    pool_size: Annotated[int, Field(5, ge=2)]
    max_overflow: int = 5


class UvicornConfig(BaseModel):
    host: str = "127.0.0.1"
    port: int = 8000
    workers: int = int(os.environ.get("WEB_CONCURRENCY", 1))
    root_path: str = ""


class AccessControl(BaseModel):
    access_policy: ImportString
    args: Optional[dict[str, Any]]

    def build(self):
        return self.access_policy(**(self.args or {}))

class MetricsConfig(BaseModel):
    prometheus: bool = True

class Spec(BaseModel):
    spec: str
    validator: Optional[ImportString]

class Config(BaseModel):
    trees: list[TreeSpec]
    media_types: Optional[dict[str, dict[str, ImportString]]] = None
    file_extensions: Optional[dict[str, str]] = None
    authentication: Optional[Authentication] = None
    database: Optional[Database] = None
    # TODO: Replace Any with AccessPolicy when #1044 is merged
    access_policy: Annotated[Optional[Any], Field(alias="access_control")] = None
    response_bytesize_limit: int = 300_000_000
    allow_origins: Optional[list[str]] = None
    uvicorn: Annotated[UvicornConfig, Field(default_factory=UvicornConfig)]
    metrics: Optional[MetricsConfig] = None
    specs: Optional[list[Spec]] = None
    reject_undeclared_specs: bool = False
    expose_raw_assets: bool = True

    @field_validator("access_policy")
    @classmethod
    def check_access_policy(cls, value: Any) -> Any:
        """Convert the access policy spec into the construct instance"""
        access = AccessControl.model_validate(value)
        return access.build()

    @field_validator("trees")
    @classmethod
    def non_overlapping_trees(cls, trees: list[TreeSpec]) -> list[TreeSpec]:
        """Ensure that paths to trees do not collide"""
        paths = set()
        for path in sorted((t.segments for t in trees), key=len):
            if any(sub in paths for sub in (*sub_paths(path), path)):
                raise ValueError("Tree paths cannot be subpaths of each other")
            paths.add(path)
        return trees

    @cached_property
    def merged_trees(self) -> Any: # TODO: update when # 1047 is merged
        trees = dict(tree.tree_entry for tree in self.trees)
        if list(trees) == [()]:
            # Simple case: there is one tree, served at the root path /.
            root_tree = trees[()]
        else:
            # There are one or more tree(s) to be served at
            # sub-paths. Merged them into one root MapAdapter.
            # Map path segments to dicts containing Adapters at that path.
            root_mapping = trees.pop((), {})
            index: dict[tuple[str, ...], dict] = {(): root_mapping}
            include_routers = set()

            # for rest of trees, build up parent nodes if required
            for segments, tree in trees.items():
                for subpath in sub_paths(segments):
                    if subpath not in index:
                        mapping = {}
                        index[subpath] = mapping
                        index[subpath[:-1]][subpath[-1]] = MapAdapter(mapping)
                index[segments[:-1]][segments[-1]] = tree
                tree_routers = set(getattr(tree, "include_routers", []))
                include_routers.update(tree_routers)

            root_tree = MapAdapter(root_mapping)
            root_tree.include_routers.extend(include_routers)
        return root_tree


def read_config(src_file: str | Path) -> Config:
    src_file = Path(src_file)
    with prepend_to_sys_path(src_file if src_file.is_dir() else src_file.parent):
        with open(src_file) as src:
            return Config.model_validate(parse(src))

def build_app_kwargs(config: Config):
    trees = dict(spec.into_tree_entry() for spec in (config.trees or []))
    pass


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
        server_settings = {}
        if root_path := config.get("root_path", ""):
            server_settings["root_path"] = root_path
        server_settings["allow_origins"] = config.get("allow_origins")
        server_settings["response_bytesize_limit"] = config.get(
            "response_bytesize_limit"
        )
        server_settings["database"] = config.get("database", {})
        server_settings["reject_undeclared_specs"] = config.get(
            "reject_undeclared_specs"
        )
        server_settings["expose_raw_assets"] = config.get("expose_raw_assets")
        server_settings["metrics"] = config.get("metrics", {})
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
            default_validation_registry.register(item["spec"], validator)

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
