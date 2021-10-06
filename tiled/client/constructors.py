import collections
import collections.abc
import urllib.parse

import httpx

from .context import context_from_tree, Context, DEFAULT_TOKEN_CACHE
from .node import Node
from .utils import DEFAULT_ACCEPTED_ENCODINGS, EVENT_HOOKS
from ..utils import import_object, prepend_to_sys_path


def from_uri(
    uri,
    structure_clients="numpy",
    *,
    cache=None,
    offline=False,
    username=None,
    token_cache=DEFAULT_TOKEN_CACHE,
    special_clients=None,
    verify=True,
    authentication_uri=None,
    headers=None,
):
    """
    Connect to a Node on a local or remote server.

    Parameters
    ----------
    uri : str
        e.g. "http://localhost:8000"
    structure_clients : str or dict, optional
        Use "dask" for delayed data loading and "numpy" for immediate
        in-memory structures (e.g. normal numpy arrays, pandas
        DataFrames). For advanced use, provide dict mapping
        structure_family names ("array", "dataframe", "variable",
        "data_array", "dataset") to client objects. See
        ``Node.DEFAULT_STRUCTURE_CLIENT_DISPATCH``.
    cache : Cache, optional
    offline : bool, optional
        False by default. If True, rely on cache only.
    username : str, optional
        Username for authenticated access.
    token_cache : str, optional
        Path to directory for storing refresh tokens.
    special_clients : dict, optional
        Advanced: Map spec from the server to special client
        tree objects. See also
        ``Node.discover_special_clients()`` and
        ``Node.DEFAULT_SPECIAL_CLIENT_DISPATCH``.
    verify : bool, optional
        Verify SSL certifications. True by default. False is insecure,
        intended for development and testing only.
    authentication_uri : str, optional
        URL of authentication server
    headers : dict, optional
        Extra HTTP headers.
    """
    # The uri is expected to reach the root or /metadata/[...] route.
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
        authentication_uri=authentication_uri,
        cache=cache,
        offline=offline,
        token_cache=token_cache,
    )
    return from_context(
        context,
        structure_clients=structure_clients,
        special_clients=special_clients,
    )


def from_tree(
    tree,
    authentication=None,
    server_settings=None,
    query_registry=None,
    serialization_registry=None,
    compression_registry=None,
    structure_clients="numpy",
    *,
    cache=None,
    offline=False,
    username=None,
    special_clients=None,
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
    authentication : dict, optional
        Dict of authentication configuration.
    username : str, optional
        Username for authenticated access.
    structure_clients : str or dict, optional
        Use "dask" for delayed data loading and "numpy" for immediate
        in-memory structures (e.g. normal numpy arrays, pandas
        DataFrames). For advanced use, provide dict mapping
        structure_family names ("array", "dataframe", "variable",
        "data_array", "dataset") to client objects. See
        ``Node.DEFAULT_STRUCTURE_CLIENT_DISPATCH``.
    cache : Cache, optional
    offline : bool, optional
        False by default. If True, rely on cache only.
    special_clients : dict, optional
        Advanced: Map spec from the server to special client
        tree objects. See also
        ``Node.discover_special_clients()`` and
        ``Node.DEFAULT_SPECIAL_CLIENT_DISPATCH``.
    token_cache : str, optional
        Path to directory for storing refresh tokens.
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
        headers=headers,
    )
    return from_context(
        context,
        structure_clients=structure_clients,
        special_clients=special_clients,
    )


def from_context(
    context,
    structure_clients="numpy",
    *,
    path=None,
    special_clients=None,
):
    """
    Advanced: Connect to a Node using a custom instance of httpx.Client or httpx.AsyncClient.

    Parameters
    ----------
    context : tiled.client.context.Context
    structure_clients : str or dict, optional
        Use "dask" for delayed data loading and "numpy" for immediate
        in-memory structures (e.g. normal numpy arrays, pandas
        DataFrames). For advanced use, provide dict mapping
        structure_family names ("array", "dataframe", "variable",
        "data_array", "dataset") to client objects. See
        ``Node.DEFAULT_STRUCTURE_CLIENT_DISPATCH``.
    username : str, optional
        Username for authenticated access.
    cache : Cache, optional
    offline : bool, optional
        False by default. If True, rely on cache only.
    special_clients : dict, optional
        Advanced: Map spec from the server to special client
        tree objects. See also
        ``Node.discover_special_clients()`` and
        ``Node.DEFAULT_SPECIAL_CLIENT_DISPATCH``.
    token_cache : str, optional
        Path to directory for storing refresh tokens.
    authentication_uri : str, optional
        URL of authentication server
    """
    # Interpret structure_clients="numpy" and structure_clients="dask" shortcuts.
    if isinstance(structure_clients, str):
        structure_clients = Node.DEFAULT_STRUCTURE_CLIENT_DISPATCH[structure_clients]
    path = path or []
    # Do entrypoint discovery if it hasn't yet been done.
    if Node.DEFAULT_SPECIAL_CLIENT_DISPATCH is None:
        Node.discover_special_clients()
    special_clients = collections.ChainMap(
        special_clients or {},
        Node.DEFAULT_SPECIAL_CLIENT_DISPATCH,
    )
    content = context.get_json(f"/metadata/{'/'.join(context.path_parts)}")
    item = content["data"]
    instance = Node(
        context,
        item=item,
        path=path,
        structure_clients=structure_clients,
        special_clients=special_clients,
    )
    return instance.client_for_item(item, path=path)


def from_profile(name, structure_clients=None, **kwargs):
    """
    Build a Node based a 'profile' (a named configuration).

    List available profiles and the source filepaths from Python like:

    >>> from tiled.client.profiles import list_profiles
    >>> list_profiles()

    or from a CLI like:

    $ tiled profile list

    Or show the file contents like:

    >>> from tiled.client.profiles import load_profiles
    >>> load_profiles()

    or from a CLI like:

    $ tiled profile show PROFILE_NAME

    Any additional parameters override profile content. See from_uri for details.
    """
    # We accept structure_clients as a separate parameter so that it
    # may be invoked positionally, as in from_profile("...", "dask").
    from ..profiles import load_profiles, paths, ProfileNotFound

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
        special_clients_ = merged.pop("special_clients", None)
        if special_clients_ is not None:
            # This is a dict mapping specs like "BlueskyRun" to values. The
            # values may be client objects or importable strings.
            result = {}
            for key, value in special_clients_.items():
                if isinstance(value, str):
                    try:
                        class_ = import_object(value, accept_live_object=True)
                    except Exception:
                        breakpoint()
                        raise
                else:
                    class_ = value
                result[key] = class_
            merged["special_clients"] = result
    if "direct" in merged:
        # The profiles specifies that there is no server. We should create
        # an app ourselves and use it directly via ASGI.
        from ..config import construct_serve_tree_kwargs

        serve_tree_kwargs = construct_serve_tree_kwargs(
            merged.pop("direct", None), source_filepath=filepath
        )
        return from_tree(**serve_tree_kwargs, **merged)
    else:
        return from_uri(**merged)


def from_config(
    config,
    authentication_uri=None,
    username=None,
    cache=None,
    offline=False,
    token_cache=DEFAULT_TOKEN_CACHE,
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

    from ..config import construct_serve_tree_kwargs

    serve_tree_kwargs = construct_serve_tree_kwargs(config)
    context = context_from_tree(
        # authentication_uri=authentication_uri,
        username=username,
        cache=cache,
        offline=offline,
        token_cache=token_cache,
        **serve_tree_kwargs,
    )
    return from_context(context, **kwargs)
