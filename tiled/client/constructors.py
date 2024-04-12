import collections
import collections.abc

import httpx

from ..utils import import_object, prepend_to_sys_path
from .container import DEFAULT_STRUCTURE_CLIENT_DISPATCH, Container
from .context import DEFAULT_TIMEOUT_PARAMS, UNSET, Context
from .utils import MSGPACK_MIME_TYPE, ClientError, client_for_item, handle_error


def from_uri(
    uri,
    structure_clients="numpy",
    *,
    cache=UNSET,
    username=UNSET,
    auth_provider=UNSET,
    api_key=None,
    verify=True,
    prompt_for_reauthentication=UNSET,
    headers=None,
    timeout=None,
    include_data_sources=False,
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
        DataFrames). For advanced use, provide dict mapping a
        structure_family or a spec to a client object.
    cache : Cache, optional
    username : str, optional
        Username for authenticated access. If UNSET, use default if available
        (typically, the most recently used).
    auth_provider : str, optional
        Name of an authentication provider. IF UNSET, use default if available
        (typically, the most recently used). If None and the server supports
        multiple provides, the user will be interactively prompted to
        choose from a list.
    api_key : str, optional
        API key based authentication. Cannot mix with username/auth_provider.
    verify : bool, optional
        Verify SSL certifications. True by default. False is insecure,
        intended for development and testing only.
    prompt_for_reauthentication : bool, optional
        If True, prompt interactively for credentials if needed. If False,
        raise an error. By default, attempt to detect whether terminal is
        interactive (is a TTY).
    headers : dict, optional
        Extra HTTP headers.
    timeout : httpx.Timeout, optional
        If None, use Tiled default settings.
        (To disable timeouts, use httpx.Timeout(None)).
    include_data_sources : bool, optional
        Default False. If True, fetch information about underlying data sources.
    """
    context, node_path_parts = Context.from_any_uri(
        uri,
        api_key=api_key,
        cache=cache,
        headers=headers,
        timeout=timeout,
        verify=verify,
    )
    return from_context(
        context,
        structure_clients=structure_clients,
        prompt_for_reauthentication=prompt_for_reauthentication,
        username=username,
        auth_provider=auth_provider,
        node_path_parts=node_path_parts,
        include_data_sources=include_data_sources,
    )


def from_context(
    context,
    structure_clients="numpy",
    prompt_for_reauthentication=UNSET,
    username=UNSET,
    auth_provider=UNSET,
    node_path_parts=None,
    include_data_sources=False,
):
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
    prompt_for_reauthentication : bool, optional
        If True, prompt interactively for credentials if needed. If False,
        raise an error. By default, attempt to detect whether terminal is
        interactive (is a TTY).
    """
    if (username is not UNSET) or (auth_provider is not UNSET):
        if context.api_key is not None:
            raise ValueError("Use api_key or username/auth_provider, not both.")
    node_path_parts = node_path_parts or []
    # Do entrypoint discovery if it hasn't yet been done.
    if Container.STRUCTURE_CLIENTS_FROM_ENTRYPOINTS is None:
        Container.discover_clients_from_entrypoints()
    # Interpret structure_clients="numpy" and structure_clients="dask" shortcuts.
    if isinstance(structure_clients, str):
        structure_clients = DEFAULT_STRUCTURE_CLIENT_DISPATCH[structure_clients]
    if (
        (context.api_key is None)
        and context.server_info["authentication"]["required"]
        and (not context.server_info["authentication"]["providers"])
    ):
        raise RuntimeError(
            """This server requires API key authentication.
Set an api_key as in:

>>> c = from_uri("...", api_key="...")
"""
        )
    if username is not UNSET:
        context.authenticate(
            username=username,
            provider=auth_provider,
            prompt_for_reauthentication=prompt_for_reauthentication,
        )
    # Context ensures that context.api_uri has a trailing slash.
    item_uri = f"{context.api_uri}metadata/{'/'.join(node_path_parts)}"
    try:
        content = handle_error(
            context.http_client.get(
                item_uri,
                headers={"Accept": MSGPACK_MIME_TYPE},
                params={"include_data_sources": include_data_sources},
            )
        ).json()
    except ClientError as err:
        if (
            (err.response.status_code == httpx.codes.UNAUTHORIZED)
            and (context.api_key is None)
            and (context.http_client.auth is None)
        ):
            context.authenticate()
            params = {}
            if include_data_sources:
                params["include_data_sources"] = True
            content = handle_error(
                context.http_client.get(
                    item_uri,
                    headers={"Accept": MSGPACK_MIME_TYPE},
                    params=params,
                )
            ).json()
        else:
            raise
    item = content["data"]
    return client_for_item(
        context, structure_clients, item, include_data_sources=include_data_sources
    )


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
            cache = Cache(**cache_config)
        else:
            # Interpret this as a Cache object passed in directly.
            cache = cache_config
        merged["cache"] = cache
    timeout_config = merged.pop("timeout", None)
    if timeout_config is not None:
        if isinstance(timeout_config, httpx.Timeout):
            timeout = timeout_config
        else:
            timeout_params = DEFAULT_TIMEOUT_PARAMS.copy()
            timeout_params.update(timeout_config)
            timeout = httpx.Timeout(**timeout_params)
        merged["timeout"] = timeout
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
        # The profile specifies the server in-line.
        # Create an app and use it directly via ASGI.
        import jsonschema

        from ..config import ConfigError, schema
        from ..server.app import build_app_from_config

        config = merged.pop("direct", None)
        try:
            jsonschema.validate(instance=config, schema=schema())
        except jsonschema.ValidationError as err:
            msg = err.args[0]
            raise ConfigError(
                f"ValidationError while parsing configuration file {filepath}: {msg}"
            ) from err
        context = Context.from_app(
            build_app_from_config(config, source_filepath=filepath),
        )
        return from_context(context, **merged)
    else:
        return from_uri(**merged)
