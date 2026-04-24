"""
This module handles server configuration.

See profiles.py for client configuration.
"""

import copy
from datetime import timedelta
from functools import cached_property
from pathlib import Path
from typing import Annotated, Any, Iterator, Optional, Union

from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)

from tiled.authenticators import ProxiedOIDCAuthenticator
from tiled.server.protocols import ExternalAuthenticator, InternalAuthenticator
from tiled.type_aliases import AppTask, TaskMap

from .adapters.mapping import MapAdapter
from .catalog import from_uri, in_memory
from .media_type_registration import (
    SerializationRegistry,
    default_compression_registry,
    default_deserialization_registry,
    default_serialization_registry,
)
from .query_registration import default_query_registry
from .server.settings import get_settings
from .structures.core import Spec
from .type_aliases import EntryPointString
from .utils import parse, prepend_to_sys_path
from .validation_registration import ValidationRegistry, default_validation_registry

TREE_ALIASES = {"catalog": "tiled.catalog:from_uri"}


def sub_paths(segments: tuple[str, ...]) -> Iterator[tuple[str, ...]]:
    for i in range(len(segments)):
        yield segments[:i]


def settings_customise_sources(
    # Give env vars priority over config file.
    # https://docs.pydantic.dev/latest/concepts/pydantic_settings/#changing-priority
    cls,
    settings_cls: type[BaseSettings],
    init_settings: PydanticBaseSettingsSource,
    env_settings: PydanticBaseSettingsSource,
    dotenv_settings: PydanticBaseSettingsSource,
    file_secret_settings: PydanticBaseSettingsSource,
) -> tuple[PydanticBaseSettingsSource, ...]:
    return env_settings, init_settings, file_secret_settings


class CatalogConfig(BaseSettings):
    uri: str
    metadata: Optional[dict] = None
    specs: Optional[list[Spec]] = None
    writable_storage: Optional[list[str]] = None
    readable_storage: Optional[list[str]] = None
    init_if_not_exists: bool = False
    adapters_by_mimetype: Optional[list[EntryPointString]] = None
    top_level_access_blob: Optional[dict] = None
    mount_node: Optional[Union[str, list[str]]] = None
    catalog_pool_size: int = 5
    storage_pool_size: int = 5
    catalog_max_overflow: int = 10
    storage_max_overflow: int = 10

    model_config = SettingsConfigDict(env_prefix="TILED_CATALOG_")
    settings_customise_sources = classmethod(settings_customise_sources)


class TreeSpec(BaseModel):
    tree_type: Annotated[EntryPointString, Field(alias="tree")]
    path: str
    args: Optional[dict[str, Any]] = None

    @model_validator(mode="after")
    def check_callable(self):
        if self.args and not callable(self.tree_type):
            raise ValueError(
                f"Tree type '{self.tree_type.__class__}' is not callable and cannot take args"
            )
        return self

    @property
    def startup_tasks(self) -> list[AppTask]:
        return getattr(self.tree, "startup_tasks", [])

    @property
    def shutdown_tasks(self) -> list[AppTask]:
        return getattr(self.tree, "shutdown_tasks", [])

    @property
    def background_tasks(self) -> list[AppTask]:
        return getattr(self.tree, "background_tasks", [])

    @cached_property
    def segments(self) -> tuple[str, ...]:
        return tuple(segment for segment in self.path.split("/") if segment)

    @cached_property
    def tree(self) -> Any:
        if callable(self.tree_type):
            return self.tree_type(**self.args or {})
        return self.tree_type

    @property
    def tree_entry(self) -> tuple[tuple[str, ...], Any]:
        return (self.segments, self.tree)

    @field_validator("tree_type", mode="before")
    @classmethod
    def tree_alias(cls, value: Any) -> Any:
        return TREE_ALIASES.get(value, value)


class AuthenticationProviderSpec(BaseModel):
    provider: str
    authenticator: EntryPointString
    args: Optional[dict[str, Any]] = None

    def into_auth_entry(
        self,
    ) -> tuple[str, Union[InternalAuthenticator, ExternalAuthenticator]]:
        auth = self.authenticator(**(self.args or {}))
        if not isinstance(auth, (InternalAuthenticator, ExternalAuthenticator)):
            raise ValueError(f"Type {auth.__class__} is not a known authenticator type")
        return (self.provider, auth)


class TiledAdmin(BaseModel):
    provider: str
    id: str


class Authentication(BaseSettings):
    # Defaults are all left as None to differentiate between unset and set to the default
    providers: Optional[list[AuthenticationProviderSpec]] = None
    tiled_admins: Optional[list[TiledAdmin]] = None
    secret_keys: Optional[list[str]] = None
    allow_anonymous_access: Optional[bool] = None
    single_user_api_key: Annotated[Optional[str], Field(pattern="[a-zA-Z0-9]+")] = None
    access_token_max_age: Optional[timedelta] = None
    refresh_token_max_age: Optional[timedelta] = None
    session_max_age: Optional[timedelta] = None

    @field_validator("providers", mode="after")
    @classmethod
    def check_unique_names(
        cls, value: list[AuthenticationProviderSpec]
    ) -> list[AuthenticationProviderSpec]:
        if value is not None:
            if len(value) != len(set(s.provider for s in value)):
                raise ValueError("Authenticator provider names must be unique")
        return value or []

    @cached_property
    def authenticators(
        self,
    ) -> dict[str, Union[InternalAuthenticator, ExternalAuthenticator]]:
        return dict(auth.into_auth_entry() for auth in self.providers or ())

    @model_validator(mode="after")
    def validate_authenticators(self):
        proxied_auths = [
            auth
            for auth in self.authenticators.values()
            if isinstance(auth, ProxiedOIDCAuthenticator)
        ]

        if len(proxied_auths) >= 2:
            raise ValueError(
                "Multiple ProxiedOIDCAuthenticator instances are configured. Only one is allowed."
            )
        if len(proxied_auths) == 1 and len(self.authenticators) != len(proxied_auths):
            raise ValueError(
                "ProxiedOIDCAuthenticator must not be configured together with other authentication providers."
            )

        return self

    model_config = SettingsConfigDict(env_prefix="TILED_AUTHN")
    settings_customise_sources = classmethod(settings_customise_sources)


class Database(BaseSettings):
    uri: Optional[str] = None
    init_if_not_exists: Optional[bool] = None
    pool_pre_ping: Optional[bool] = None
    pool_size: Annotated[Optional[int], Field(ge=2)] = 5
    max_overflow: Optional[int] = 10

    model_config = SettingsConfigDict(env_prefix="TILED_AUTHN_DATABASSE")
    settings_customise_sources = classmethod(settings_customise_sources)


class AccessControl(BaseSettings):
    access_policy: EntryPointString
    args: Optional[dict[str, Any]]

    def build(self):
        return self.access_policy(**(self.args or {}))

    model_config = SettingsConfigDict(env_prefix="TILED_ACCESS_CONTROL")
    settings_customise_sources = classmethod(settings_customise_sources)


class MetricsConfig(BaseSettings):
    prometheus: bool = True

    model_config = SettingsConfigDict(env_prefix="TILED_METRICS")
    settings_customise_sources = classmethod(settings_customise_sources)


class ValidationSpec(BaseSettings):
    spec: str
    validator: Optional[EntryPointString] = None

    model_config = SettingsConfigDict(env_prefix="TILED_VALIDATORS")
    settings_customise_sources = classmethod(settings_customise_sources)


class StreamingCacheConfig(BaseSettings):
    uri: str
    data_ttl: int = 3600  # 1 hr
    seq_ttl: int = 2592000  # 30 days
    socket_timeout: int = 86400  # 1 day
    socket_connect_timeout: int = 10

    model_config = SettingsConfigDict(env_prefix="TILED_STREAMING_CACHE_")
    settings_customise_sources = classmethod(settings_customise_sources)


class WebhooksConfig(BaseSettings):
    """Configuration for the optional webhooks feature.

    When this section is present in the server configuration, the
    ``/api/v1/webhooks`` router is mounted and the WebhookDispatcher is
    started alongside the catalog.  When absent, webhooks are fully disabled.

    Parameters
    ----------
    secret_keys : list of str
        Keys used to encrypt webhook HMAC signing secrets at rest.
        Required; generate one with ``openssl rand -hex 32``.
    """

    secret_keys: list[str]

    model_config = SettingsConfigDict(env_prefix="TILED_WEBHOOKS_")
    settings_customise_sources = classmethod(settings_customise_sources)


class Config(BaseSettings):
    catalog: Optional[CatalogConfig] = None  # recommended
    trees: Optional[list[TreeSpec]] = None  # advanced alternative
    media_types: dict[str, dict[str, EntryPointString]] = {}
    file_extensions: dict[str, str] = {}
    authentication: Authentication = Authentication()
    database: Optional[Database] = None
    # TODO: Replace Any with AccessPolicy when #1044 is merged
    access_policy: Annotated[Optional[Any], Field(alias="access_control")] = None
    response_bytesize_limit: int = 300_000_000
    exact_count_limit: Optional[int] = None
    allow_origins: Optional[list[str]] = None
    uvicorn: dict[str, Any] = {}
    metrics: MetricsConfig = MetricsConfig()
    specs: list[ValidationSpec] = []
    reject_undeclared_specs: bool = False
    expose_raw_assets: bool = True
    routers: list[EntryPointString] = []
    streaming_cache: Optional[StreamingCacheConfig] = None
    webhooks: Optional[WebhooksConfig] = None
    create_mount_nodes_if_not_exist: bool = False

    # If recommended 'catalog' config is used, these parameters are
    # not used; they are set inside the CatalogConfig.
    # If legacy / flexible 'trees' config is used, these are used.
    catalog_pool_size: int = 5
    storage_pool_size: int = 5
    catalog_max_overflow: int = 10
    storage_max_overflow: int = 10

    model_config = SettingsConfigDict(env_prefix="TILED_")
    settings_customise_sources = classmethod(settings_customise_sources)

    @field_validator("access_policy")
    @classmethod
    def check_access_policy(cls, value: Any) -> Any:
        """Convert the access policy spec into the construct instance"""
        if value is None:
            return None
        access = AccessControl.model_validate(value)
        return access.build()

    @model_validator(mode="after")
    def populate_streaming_cache_from_env(self) -> "Config":
        if self.streaming_cache is None:
            try:
                self.streaming_cache = StreamingCacheConfig()
            except ValidationError:
                pass  # uri not set, leave as None
        return self

    @model_validator(mode="after")
    def populate_webhooks_from_env(self) -> "Config":
        if self.webhooks is None:
            try:
                self.webhooks = WebhooksConfig()
            except ValidationError:
                pass  # secret_keys not set, leave as None
        return self

    @model_validator(mode="after")
    def reconcile_catalog_and_trees(self):
        """
        The 'catalog' and 'trees' config are mutually exclusive.

        The 'catalog' (recommended) specifies one catalog; 'trees' (advanced)
        can specify multiple catalogs, mounted at different prefixes.
        """
        if self.catalog is not None:
            if self.trees:
                raise ValueError(
                    "The configuration 'catalog' specifies a single catalog, "
                    "whereas 'trees' can specify multiple catalogs. "
                    "It is not allowed to use both 'catalog' and 'trees'."
                )
            # Build a TreeSpec out of the catalog.
            tree = TreeSpec(
                tree="catalog",
                path="/",
                args=self.catalog.model_dump(),
            )
            self.trees = [tree]
            # Having parsed the catalog into a tree, put the model in a
            # self-consistent state.
            # (The model cannot have *both* catalog and trees.)
            self.catalog = None
        elif not self.trees:
            raise ValueError(
                "Configuration must specific 'catalog' or 'trees' (multiple catalogs)."
            )
        return self

    @field_validator("trees")
    @classmethod
    def non_overlapping_trees(cls, trees: list[TreeSpec]) -> list[TreeSpec]:
        """Ensure that paths to trees do not collide"""
        paths = set()
        for path in sorted((t.segments for t in (trees or [])), key=len):
            for sub in (*sub_paths(path), path):
                if sub in paths:
                    raise ValueError(
                        f"Tree paths cannot be subpaths of each other: '/{'/'.join(sub)}' and '/{'/'.join(path)}'"
                    )
            paths.add(path)
        return trees

    @model_validator(mode="after")
    def fudge_tree_args(self):
        # Needing to fudge the args of tree specs is awful
        for tree in self.trees or []:
            tree.args = tree.args or {}
            if tree.tree_type is from_uri:
                defaults = get_settings()
                # Assumes none of the values can be 0
                tree.args["catalog_pool_size"] = (
                    self.catalog_pool_size or defaults.catalog_pool_size
                )
                tree.args["storage_pool_size"] = (
                    self.storage_pool_size or defaults.storage_pool_size
                )
                tree.args["catalog_max_overflow"] = (
                    self.catalog_max_overflow or defaults.catalog_max_overflow
                )
                tree.args["storage_max_overflow"] = (
                    self.storage_max_overflow or defaults.storage_max_overflow
                )
            if tree.tree_type in (from_uri, in_memory):
                tree.args["cache_config"] = (
                    self.streaming_cache.model_dump() if self.streaming_cache else None
                )
                tree.args["webhook_secret_keys"] = (
                    list(self.webhooks.secret_keys) if self.webhooks else None
                )
            if tree.tree_type is from_uri:
                tree.args[
                    "create_mount_nodes_if_not_exist"
                ] = self.create_mount_nodes_if_not_exist
        return self

    @property
    def root_path(self) -> str:
        return self.uvicorn.get("root_path") or ""

    @cached_property
    def merged_trees(self) -> Any:  # TODO: update when # 1047 is merged
        trees = {
            segments: tree
            for segments, tree in (tree_spec.tree_entry for tree_spec in self.trees)
        }
        if list(trees) == [()]:
            # Simple case: there is one tree, served at the root path /.
            root_tree = trees[()]
        else:
            # There are one or more tree(s) to be served at sub-paths so merge
            # them into one root MapAdapter with map path segments to dicts
            # containing Adapters at that path.
            root_mapping = trees.pop((), {})
            index: dict[tuple[str, ...], dict] = {(): root_mapping}

            # for rest of trees, build up parent nodes if required
            for segments, tree in trees.items():
                for subpath in sub_paths(segments):
                    if subpath not in index:
                        mapping = {}
                        index[subpath] = mapping
                        index[subpath[:-1]][subpath[-1]] = MapAdapter(mapping)
                index[segments[:-1]][segments[-1]] = tree
                self.routers.extend(getattr(tree, "include_routers", []))
            root_tree = MapAdapter(root_mapping)

        return root_tree

    def tree_tasks(self) -> TaskMap:
        startup = []
        shutdown = []
        background = []
        for tree in self.trees:
            startup.extend(tree.startup_tasks)
            shutdown.extend(tree.shutdown_tasks)
            background.extend(tree.background_tasks)
        return {
            "startup": startup,
            "shutdown": shutdown,
            "background": background,
        }

    def serialization_registry(self) -> SerializationRegistry:
        base = copy.deepcopy(default_serialization_registry)
        for family, types in self.media_types.items():
            for typ, func in types.items():
                base.register(family, typ, func)
        for ext, media_type in self.file_extensions.items():
            base.register_alias(ext, media_type)
        return base

    def validation_registry(self) -> ValidationRegistry:
        base = copy.deepcopy(default_validation_registry)
        for spec in self.specs:
            base.register(Spec(spec.spec), spec.validator or _no_op_validator)
        return base


def parse_configs(src_file: Union[str, Path]) -> Config:
    src_file = Path(src_file)
    if src_file.is_dir():
        conf = {}
        for f in src_file.iterdir():
            if f.is_file() and f.suffix in (".yml", ".yaml"):
                new_config = parse(f)
                if common := new_config.keys() & conf.keys():
                    # These specific keys can be merged from separate files.
                    # This can be useful for config.d-style where different
                    # files are managed by different stages of configuration
                    # management.
                    mergeable_lists = {"allow_origins", "specs", "trees"}
                    for key in common.intersection(mergeable_lists):
                        conf[key].extend(new_config.pop(key))
                        common.remove(key)
                    if common:
                        raise ValueError(f"Duplicate configuration for {common} in {f}")
                conf.update(new_config)
    else:
        conf = parse(src_file)

    with prepend_to_sys_path(src_file if src_file.is_dir() else src_file.parent):
        return Config.model_validate(conf)


def construct_build_app_kwargs(config: Config):
    server_settings = dict(
        root_path=config.root_path,
        allow_origins=config.allow_origins,
        response_bytesize_limit=config.response_bytesize_limit,
        exact_count_limit=config.exact_count_limit,
        database=config.database,
        reject_undeclared_specs=config.reject_undeclared_specs,
        expose_raw_assets=config.expose_raw_assets,
        metrics=config.metrics,
        webhooks=config.webhooks,
    )
    return dict(
        tree=config.merged_trees,
        authentication=config.authentication,
        server_settings=server_settings,
        query_registry=default_query_registry,
        serialization_registry=config.serialization_registry(),
        deserialization_registry=default_deserialization_registry,
        compression_registry=default_compression_registry,
        validation_registry=config.validation_registry(),
        tasks=config.tree_tasks(),
        access_policy=config.access_policy,
        include_routers=config.routers,
    )


def _no_op_validator(*args, **kwargs):
    return None
