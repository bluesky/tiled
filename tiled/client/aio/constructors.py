"""EXPERIMENTAL: entry points for connecting an asynchronous client."""

from __future__ import annotations

from typing import Any, Optional

import httpx

from ...utils import UNSET
from .._protocol.node import draft_request_refresh_node
from ..utils import client_for_item
from .context import AsyncContext
from .dispatch import default_structure_clients


async def from_uri(
    uri: str,
    *,
    api_key: Optional[str] = None,
    structure_clients: Any = None,
    include_data_sources: bool = False,
    timeout: Optional[httpx.Timeout] = None,
    verify: bool = True,
):
    """Connect to a Node on a local or remote server, asynchronously.

    The returned client holds the connection open; close it by closing its
    context (`await client.context.aclose()`), most easily by using the
    client's context as an async context manager:

    >>> client = await from_uri("https://tiled.example.com", api_key=...)
    >>> async with client.context:
    ...     ...
    """
    context = await AsyncContext.connect(
        uri, api_key=api_key, timeout=timeout, verify=verify
    )
    try:
        return await from_context(
            context,
            structure_clients=structure_clients,
            include_data_sources=include_data_sources,
        )
    except BaseException:
        await context.aclose()
        raise


async def from_app(
    app: Any,
    *,
    api_key: Any = UNSET,
    structure_clients: Any = None,
    include_data_sources: bool = False,
    run_lifespan: bool = True,
):
    "Connect to a Node of an in-process ASGI app, asynchronously. For testing."
    context = await AsyncContext.from_app(
        app, api_key=api_key, run_lifespan=run_lifespan
    )
    try:
        return await from_context(
            context,
            structure_clients=structure_clients,
            include_data_sources=include_data_sources,
        )
    except BaseException:
        await context.aclose()
        raise


async def from_context(
    context: AsyncContext,
    *,
    structure_clients: Any = None,
    node_path_parts: Optional[list[str]] = None,
    include_data_sources: bool = False,
):
    "Build a client for the node at `node_path_parts` (default: the root node)."
    if structure_clients is None:
        structure_clients = default_structure_clients()
    node_path_parts = node_path_parts or []
    item_uri = f"{context.api_uri}metadata/{'/'.join(node_path_parts)}"
    draft = draft_request_refresh_node(item_uri, include_data_sources)
    content = (await context.send(draft)).json()
    return client_for_item(
        context,
        structure_clients,
        content["data"],
        include_data_sources=include_data_sources,
    )
