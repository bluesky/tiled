import abc
from collections import defaultdict
import dataclasses
import math
import operator
import re
from typing import Any

import dask.base
from fastapi import Depends, HTTPException, Query, Response
import msgpack
from starlette.responses import JSONResponse, StreamingResponse, Send

from . import models
from .authentication import get_current_user
from .settings import get_settings
from .. import queries  # This is not used, but it registers queries on import.
from ..query_registration import name_to_query_type
from ..media_type_registration import serialization_registry

del queries


_FILTER_PARAM_PATTERN = re.compile(r"filter___(?P<name>.*)___(?P<field>[^\d\W][\w\d]+)")


# @lru_cache()
# def get_dask_client():
#     "Connect to a specified dask scheduler, or start a LocalCluster."
#     address = get_settings().dask_scheduler_address
#     if address:
#         # Connect to an existing cluster.
#         client = Client(address, asynchronous=True)
#     else:
#         # Start a distributed.LocalCluster.
#         client = Client(asynchronous=True, processes=False)
#     return client


def get_entry(path, current_user):
    root_catalog = get_settings().catalog
    catalog = root_catalog.authenticated_as(current_user)
    # Traverse into sub-catalog(s).
    for entry in (path or "").split("/"):
        if entry:
            try:
                catalog = catalog[entry]
            except (KeyError, TypeError):
                raise NoEntry(path)
    return catalog


def entry(
    path: str,
    current_user: str = Depends(get_current_user),
):
    try:
        return get_entry(path, current_user)
    except NoEntry:
        raise HTTPException(status_code=404, detail=f"No such entry: {path}")


def datasource(
    entry: Any = Depends(entry),
):
    "Specify a path parameter and use it to look up a datasource."
    if not isinstance(entry, DuckDataSource):
        raise HTTPException(
            status_code=404, detail="This is a Catalog, not a DataSource."
        )
    return entry


def block(
    # Ellipsis as the "default" tells FastAPI to make this parameter required.
    block: str = Query(..., min_length=1, regex="^[0-9](,[0-9])*$"),
):
    "Specify and parse a block index parameter."
    parsed_block = tuple(map(int, block.split(",")))
    return parsed_block


def len_or_approx(catalog):
    try:
        return len(catalog)
    except TypeError:
        return operator.length_hint(catalog)


def get_chunk(chunk):
    "dask array -> numpy array"
    return chunk.compute(scheduler="threads")


def pagination_links(route, path, offset, limit, length_hint):
    # TODO Include root path in links.
    # root_path = request.scope.get("/")
    links = {
        "self": f"{route}{path}?page[offset]={offset}&page[limit]={limit}",
        # These are conditionally overwritten below.
        "first": None,
        "last": None,
        "next": None,
        "prev": None,
    }
    if limit:
        last_page = math.floor(length_hint / limit) * limit
        links.update(
            {
                "first": f"{route}{path}?page[offset]={0}&page[limit]={limit}",
                "last": f"{route}{path}?page[offset]={last_page}&page[limit]={limit}",
            }
        )
    if offset + limit < length_hint:
        links[
            "next"
        ] = f"{route}{path}?page[offset]={offset + limit}&page[limit]={limit}"
    if offset > 0:
        links[
            "prev"
        ] = f"{route}{path}?page[offset]={max(0, offset - limit)}&page[limit]={limit}"
    return links


class DuckDataSource(metaclass=abc.ABCMeta):
    """
    Used for isinstance(obj, DuckDataSource):
    """

    @classmethod
    def __subclasshook__(cls, candidate):
        # If the following condition is True, candidate is recognized
        # to "quack" like a DataSource.
        EXPECTED_ATTRS = (
            "read",
            "describe",
        )
        return all(hasattr(candidate, attr) for attr in EXPECTED_ATTRS)


class DuckCatalog(metaclass=abc.ABCMeta):
    """
    Used for isinstance(obj, DuckCatalog):
    """

    @classmethod
    def __subclasshook__(cls, candidate):
        # If the following condition is True, candidate is recognized
        # to "quack" like a Catalog.
        EXPECTED_ATTRS = (
            "__getitem__",
            "__iter__",
        )
        return all(hasattr(candidate, attr) for attr in EXPECTED_ATTRS)


def construct_entries_response(
    route,
    path,
    offset,
    limit,
    fields,
    filters,
    current_user,
):
    path = path.rstrip("/")
    if not path.startswith("/"):
        path = f"/{path}"
    catalog = get_entry(path, current_user)
    if not isinstance(catalog, DuckCatalog):
        raise WrongTypeForRoute("This is a Data Source, not a Catalog.")
    queries = defaultdict(
        dict
    )  # e.g. {"text": {"text": "dog"}, "lookup": {"key": "..."}}
    # Group the parameters by query type.
    for key, value in filters.items():
        if value is None:
            continue
        name, field = _FILTER_PARAM_PATTERN.match(key).groups()
        queries[name][field] = value
    # Apply the queries and obtain a narrowed catalog.
    for name, parameters in queries.items():
        query_class = name_to_query_type[name]
        query = query_class(**parameters)
        catalog = catalog.search(query)
    count = len_or_approx(catalog)
    links = pagination_links(route, path, offset, limit, count)
    data = []
    if fields:
        # Pull a page of items into memory.
        items = catalog.items_indexer[offset : offset + limit]
    else:
        # Pull a page of just the keys, which is cheaper.
        items = ((key, None) for key in catalog.keys_indexer[offset : offset + limit])
    for key, entry in items:
        resource = construct_resource(key, entry, fields)
        data.append(resource)
    return models.Response(data=data, links=links, meta={"count": count})


def construct_array_response(array, request_headers):
    DEFAULT_MEDIA_TYPE = "application/octet-stream"
    etag = dask.base.tokenize(array)
    if request_headers.get("If-None-Match", "") == etag:
        return Response(status_code=304)
    media_types = request_headers.get("Accept", DEFAULT_MEDIA_TYPE).split(", ")
    for media_type in media_types:
        if media_type == "*/*":
            media_type = DEFAULT_MEDIA_TYPE
        if media_type in serialization_registry.media_types("array"):
            content = serialization_registry("array", media_type, array)
            return PatchedResponse(
                content=content, media_type=media_type, headers={"ETag": etag}
            )
    else:
        raise UnsupportedMediaTypes(
            "None of the media types requested by the client are supported.",
            unsupported=media_types,
            supported=serialization_registry.media_types("array"),
        )


def construct_resource(key, entry, fields):
    attributes = {}
    if models.EntryFields.metadata in fields:
        attributes["metadata"] = entry.metadata
    if models.EntryFields.client_type_hint in fields:
        attributes["client_type_hint"] = getattr(entry, "client_type_hint", None)
    if isinstance(entry, DuckCatalog):
        if models.EntryFields.count in fields:
            attributes["count"] = len_or_approx(entry)
        resource = models.CatalogResource(
            **{
                "id": key,
                "attributes": models.CatalogAttributes(**attributes),
                "type": models.EntryType.catalog,
            }
        )
    else:
        if models.EntryFields.container in fields:
            attributes["container"] = entry.container
        if models.EntryFields.structure in fields:
            attributes["structure"] = dataclasses.asdict(entry.describe())
        resource = models.DataSourceResource(
            **{
                "id": key,
                "attributes": models.DataSourceAttributes(**attributes),
                "type": models.EntryType.datasource,
            }
        )
    return resource


class PatchedResponse(Response):
    "Patch the render method to accept memoryview."

    def render(self, content: Any) -> bytes:
        if isinstance(content, memoryview):
            return content.cast("B")
        return super().render(content)


class PatchedStreamingResponse(StreamingResponse):
    "Patch the stream_response method to accept memoryview."

    async def stream_response(self, send: Send) -> None:
        await send(
            {
                "type": "http.response.start",
                "status": self.status_code,
                "headers": self.raw_headers,
            }
        )
        async for chunk in self.body_iterator:
            # BEGIN ALTERATION
            if not isinstance(chunk, (bytes, memoryview)):
                # END ALTERATION
                chunk = chunk.encode(self.charset)
            await send({"type": "http.response.body", "body": chunk, "more_body": True})

        await send({"type": "http.response.body", "body": b"", "more_body": False})


class MsgpackResponse(Response):
    media_type = "application/x-msgpack"

    def render(self, content: Any) -> bytes:
        return msgpack.packb(content)


def json_or_msgpack(request_headers, content):
    DEFAULT_MEDIA_TYPE = "application/json"
    media_types = request_headers.get("Accept", DEFAULT_MEDIA_TYPE).split(", ")
    for media_type in media_types:
        if media_type == "*/*":
            media_type = DEFAULT_MEDIA_TYPE
        if media_type == "application/x-msgpack":
            return MsgpackResponse(content.dict())
        if media_type == "application/json":
            return JSONResponse(content.dict())
    else:
        raise UnsupportedMediaTypes(
            "None of the media types requested by the client are supported.",
            unsupported=media_types,
            supported=["application/json", "application/x-msgpack"],
        )


class UnsupportedMediaTypes(Exception):
    pass


class NoEntry(KeyError):
    pass


class WrongTypeForRoute(Exception):
    pass
