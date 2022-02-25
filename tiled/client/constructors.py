import collections
import collections.abc
import urllib.parse

import httpx

from ..utils import import_object, prepend_to_sys_path
from .context import (
    DEFAULT_TOKEN_CACHE,
    Context,
    PromptForReauthentication,
    context_from_tree,
)
from .node import Node
from .utils import DEFAULT_ACCEPTED_ENCODINGS, EVENT_HOOKS


def from_uri(
    uri,
    structure_clients="numpy",
    *,
    cache=None,
    offline=False,
    username=None,
    auth_provider=None,
    api_key=None,
    token_cache=DEFAULT_TOKEN_CACHE,
    verify=True,
    prompt_for_reauthentication=PromptForReauthentication.AT_INIT,
    headers=None,
):
    """
    Connect to a Node on a local or remote server.

    Parameters
    ----------
    uri : str
        e.g. "http://localhost:8000/api"
    structure_clients : str or dict, optional
        Use "dask" for delayed data loading and "numpy" for immediate
        in-memory structures (e.g. normal numpy arrays, pandas
        DataFrames). For advanced use, provide dict mapping a
        structure_family or a spec to a client object.
    cache : Cache, optional
    offline : bool, optional
        False by default. If True, rely on cache only.
    username : str, optional
        Username for authenticated access.
    auth_provider : str, optional
        Name of an authentication provider. If None and the server supports
        multiple provides, the user will be interactively prompted to
        choose from a list.
    api_key : str, optional
        API key based authentication. Cannot mix with username/auth_provider.
    token_cache : str, optional
        Path to directory for storing refresh tokens.
    verify : bool, optional
        Verify SSL certifications. True by default. False is insecure,
        intended for development and testing only.
    prompt_for_reauthentication : {"at_init", "always", "never"}
    headers : dict, optional
        Extra HTTP headers.
    """
    # The uri is expected to reach the root or /node/metadata/[...] route.
    url = httpx.URL(uri)
    headers = headers or {}
    headers.setdefault("accept-encoding", ",".join(DEFAULT_ACCEPTED_ENCODINGS))
    params = {}
    # If ?api_key=... is present, move it from the query into a header.
    # The server would accept it in the query parameter, but using
    # a header is a little more secure (e.g. not logged) and makes
    # it is simpler to manage the client.base_url.
    parsed_query = urllib.parse.parse_qs(url.query.decode())
    api_key_list = parsed_query.pop("api_key", None)
    if api_key_list is not None:
        if len(api_key_list) != 1:
            raise ValueError("Cannot handle two api_key query parameters")
        (api_key,) = api_key_list
        headers["X-TILED-API-KEY"] = api_key
    params.update(urllib.parse.urlencode(parsed_query, doseq=True))
    # Construct the URL *without* the params, which we will pass in separately.
    base_uri = urllib.parse.urlunsplit(
        (url.scheme, url.netloc.decode(), url.path, {}, url.fragment)
    )

    client = httpx.Client(
        base_url=base_uri,
        verify=verify,
        event_hooks=EVENT_HOOKS,
        timeout=httpx.Timeout(5.0, read=20.0),
        headers=headers,
        params=params,
    )
    context = Context(
        client,
        username=username,
        auth_provider=auth_provider,
        api_key=api_key,
        cache=cache,
        offline=offline,
        token_cache=token_cache,
        prompt_for_reauthentication=prompt_for_reauthentication,
    )
    return from_context(context, structure_clients=structure_clients)


def from_tree(
    tree,
    structure_clients="numpy",
    *,
    authentication=None,
    server_settings=None,
    query_registry=None,
    serialization_registry=None,
    compression_registry=None,
    cache=None,
    offline=False,
    username=None,
    auth_provider=None,
    api_key=None,
    token_cache=DEFAULT_TOKEN_CACHE,
    headers=None,
):
    """
    Connect to a Node directly, running the app in this same process.

    NOTE: This is experimental. It may need to be re-designed or even removed.

    In this configuration, we are using the server, but we are communicating
    with it directly within this process, not over a local network. It is
    generally faster.

    Specifically, we are using HTTP over ASGI rather than HTTP over TCP.
    There are no sockets or network-related syscalls.

    Parameters
    ----------
    tree : Node
    structure_clients : str or dict, optional
        Use "dask" for delayed data loading and "numpy" for immediate
        in-memory structures (e.g. normal numpy arrays, pandas
        DataFrames). For advanced use, provide dict mapping a
        structure_family or a spec to a client object.
    authentication : dict, optional
        Dict of authentication configuration.
    username : str, optional
        Username for authenticated access.
    auth_provider : str, optional
        Name of an authentication provider. If None and the server supports
        multiple provides, the user will be interactively prompted to
        choose from a list.
    api_key : str, optional
        API key based authentication. Cannot mix with username/auth_provider.
    cache : Cache, optional
    offline : bool, optional
        False by default. If True, rely on cache only.
    token_cache : str, optional
        Path to directory for storing refresh tokens.
    prompt_for_reauthentication : {"at_init", "always", "never"}
    """
    context = context_from_tree(
        tree=tree,
        authentication=authentication,
        server_settings=server_settings,
        query_registry=query_registry,
        serialization_registry=serialization_registry,
        compression_registry=compression_registry,
        # The cache and "offline" mode do not make much sense when we have an
        # in-process connection, but we support it for the sake of testing and
        # making direct access a drop in replacement for the normal service.
        cache=cache,
        offline=offline,
        token_cache=token_cache,
        username=username,
        auth_provider=auth_provider,
        api_key=api_key,
        headers=headers,
    )
    return from_context(context, structure_clients=structure_clients)


def from_context(context, structure_clients="numpy", *, path=None):
    """
    Advanced: Connect to a Node using a custom instance of httpx.Client or httpx.AsyncClient.

    Parameters
    ----------
    context : tiled.client.context.Context
    structure_clients : str or dict, optional
        Use "dask" for delayed data loading and "numpy" for immediate
        in-memory structures (e.g. normal numpy arrays, pandas
        DataFrames). For advanced use, provide dict mapping a
        structure_family or a spec to a client object.
    """
    # Do entrypoint discovery if it hasn't yet been done.
    if Node.STRUCTURE_CLIENTS_FROM_ENTRYPOINTS is None:
        Node.discover_clients_from_entrypoints()
    # Interpret structure_clients="numpy" and structure_clients="dask" shortcuts.
    if isinstance(structure_clients, str):
        structure_clients = Node.DEFAULT_STRUCTURE_CLIENT_DISPATCH[structure_clients]
    path = path or []
    content = context.get_json(f"/node/metadata/{'/'.join(context.path_parts)}")
    item = content["data"]
    instance = Node(
        context,
        item=item,
        path=path,
        structure_clients=structure_clients,
    )
    return instance.client_for_item(item, path=path)


def from_profile(name, structure_clients=None, **kwargs):
    """
    Build a Node based a 'profile' (a named configuration).

    List available profiles and the source filepaths from Python like:

    >>> from tiled.profiles import list_profiles
    >>> list_profiles()

    or from a CLI like:

    $ tiled profile list

    Or show the file contents like:

    >>> from tiled.profiles import load_profiles
    >>> load_profiles()

    or from a CLI like:

    $ tiled profile show PROFILE_NAME

    Any additional parameters override profile content. See from_uri for details.
    """
    # We accept structure_clients as a separate parameter so that it
    # may be invoked positionally, as in from_profile("...", "dask").
    from ..profiles import ProfileNotFound, load_profiles, paths

    profiles = load_profiles()
    try:
        filepath, profile_content = profiles[name]
    except KeyError as err:
        raise ProfileNotFound(
            f"Profile {name!r} not found. Found profiles {list(profiles)} "
            f"from directories {paths}."
        ) from err
    merged = {**profile_content, **kwargs}
    if structure_clients is not None:
        merged["structure_clients"] = structure_clients
    cache_config = merged.pop("cache", None)
    if cache_config is not None:
        from tiled.client.cache import Cache

        if isinstance(cache_config, collections.abc.Mapping):
            # All necessary validation has already been performed
            # in load_profiles().
            ((key, value),) = cache_config.items()
            # For back-compat, rename "available_bytes" to "capacity".
            available_bytes = value.pop("available_bytes", None)
            if available_bytes:
                if "capacity" in value:
                    raise ValueError(
                        "Cannot specific both 'capacity' and its deprecated alias 'available_bytes'."
                    )
                value["capacity"] = available_bytes
                import warnings

                warnings.warn(
                    "Profile specifies 'available_bytes'. Use new name 'capacity' instead. "
                    "Support for the old name, 'available_bytes', will be removed in the future."
                )
            if key == "memory":
                cache = Cache.in_memory(**value)
            elif key == "disk":
                cache = Cache.on_disk(**value)
        else:
            # Interpret this as a Cache object passed in directly.
            cache = cache_config
        merged["cache"] = cache
    # Below, we may convert importable strings like
    # "package.module:obj" to live objects. Include the profile's
    # source directory in the import path, temporarily.
    with prepend_to_sys_path(filepath.parent):
        structure_clients_ = merged.pop("structure_clients", None)
        if structure_clients_ is not None:
            if isinstance(structure_clients_, str):
                # Nothing to do.
                merged["structure_clients"] = structure_clients_
            else:
                # This is a dict mapping structure families like "array" and "dataframe"
                # to values. The values may be client objects or importable strings.
                result = {}
                for key, value in structure_clients_.items():
                    if isinstance(value, str):
                        class_ = import_object(value, accept_live_object=True)
                    else:
                        class_ = value
                    result[key] = class_
                merged["structure_clients"] = result
    if "direct" in merged:
        # The profiles specifies that there is no server. We should create
        # an app ourselves and use it directly via ASGI.
        from ..config import construct_build_app_kwargs

        build_app_kwargs = construct_build_app_kwargs(
            merged.pop("direct", None), source_filepath=filepath
        )
        return from_tree(**build_app_kwargs, **merged)
    else:
        return from_uri(**merged)


def from_config(
    config,
    *,
    username=None,
    auth_provider=None,
    api_key=None,
    cache=None,
    offline=False,
    token_cache=DEFAULT_TOKEN_CACHE,
    prompt_for_reauthentication=PromptForReauthentication.AT_INIT,
    **kwargs,
):
    """
    Build Nodes directly, running the app in this same process.

    NOTE: This is experimental. It may need to be re-designed or even removed.

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
                        "tree": "tiled.files.Node.from_files",
                        "args": {"diretory": "path/to/files"}
                    ]
            }
        )
    """

    from ..config import construct_build_app_kwargs

    build_app_kwargs = construct_build_app_kwargs(config)
    context = context_from_tree(
        username=username,
        auth_provider=auth_provider,
        api_key=api_key,
        cache=cache,
        offline=offline,
        token_cache=token_cache,
        prompt_for_reauthentication=prompt_for_reauthentication,
        **build_app_kwargs,
    )
    return from_context(context, **kwargs)
