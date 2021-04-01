import abc
from collections import defaultdict
import dataclasses
from hashlib import md5
import math
from mimetypes import types_map
import operator
import re
from typing import Any

import dask.base
from fastapi import Depends, HTTPException, Query, Response
import msgpack
import pydantic
from starlette.responses import JSONResponse, StreamingResponse, Send

from . import models
from .authentication import get_current_user
from .settings import get_settings
from ..utils import modules_available
from ..query_registration import name_to_query_type
from ..media_type_registration import serialization_registry


# These modules are not directly used, but they register things on import.
from .. import queries

del queries
if modules_available("numpy", "dask.array"):
    from ..structures import array as _array  # noqa: F401

    del _array
if modules_available("pandas", "pyarrow", "dask.dataframe"):
    from ..structures import dataframe as _dataframe  # noqa: F401

    del _dataframe
if modules_available("xarray"):
    from ..structures import xarray as _xarray  # noqa: F401

    del _xarray


_FILTER_PARAM_PATTERN = re.compile(r"filter___(?P<name>.*)___(?P<field>[^\d\W][\w\d]+)")


def entry(
    path: str,
    current_user: str = Depends(get_current_user),
    settings: pydantic.BaseSettings = Depends(get_settings),
):
    try:
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
    except NoEntry:
        raise HTTPException(status_code=404, detail=f"No such entry: {path}")


def reader(
    entry: Any = Depends(entry),
):
    "Specify a path parameter and use it to look up a reader."
    if not isinstance(entry, DuckReader):
        raise HTTPException(status_code=404, detail="This is not a Reader.")
    return entry


def block(
    # Ellipsis as the "default" tells FastAPI to make this parameter required.
    block: str = Query(..., min_length=1, regex="^[0-9](,[0-9])*$"),
):
    "Specify and parse a block index parameter."
    parsed_block = tuple(map(int, block.split(",")))
    return parsed_block


def slice_(
    slice: str = Query(None, regex="^[0-9,:]*$"),
):
    "Specify and parse a block index parameter."
    import numpy

    # IMPORTANT We are eval-ing a user-provider string here so we need to be
    # very careful about locking down what can be in it. The regex above
    # excludes any letters or operators, should it is not possible to execute
    # functions or expensive artithmetic.
    return tuple(
        [
            eval(f"numpy.s_[{dim!s}]", {"numpy": numpy})
            for dim in (slice or "").split(",")
            if dim
        ]
    )


def len_or_approx(catalog):
    try:
        return len(catalog)
    except TypeError:
        return operator.length_hint(catalog)


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


class DuckReader(metaclass=abc.ABCMeta):
    """
    Used for isinstance(obj, DuckReader):
    """

    @classmethod
    def __subclasshook__(cls, candidate):
        # If the following condition is True, candidate is recognized
        # to "quack" like a Reader.
        EXPECTED_ATTRS = (
            "read",
            "macrostructure",
            "microstructure",
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
    catalog,
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
    if not isinstance(catalog, DuckCatalog):
        raise WrongTypeForRoute("This is not a Catalog.")
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
    if fields != [models.EntryFields.none]:
        # Pull a page of items into memory.
        items = catalog.items_indexer[offset : offset + limit]  # noqa: E203
    else:
        # Pull a page of just the keys, which is cheaper.
        items = (
            (key, None)
            for key in catalog.keys_indexer[offset : offset + limit]  # noqa: E203
        )
    for key, entry in items:
        resource = construct_resource(path, key, entry, fields)
        data.append(resource)
    return models.Response(data=data, links=links, meta={"count": count})


def construct_array_response(array, request_headers, format=None):
    import numpy

    DEFAULT_MEDIA_TYPE = "application/octet-stream"
    # Ensure contiguous C-ordered array.
    array = numpy.ascontiguousarray(array)
    etag = dask.base.tokenize(array)
    if request_headers.get("If-None-Match", "") == etag:
        return Response(status_code=304)
    # Give priority to the `format` query parameter. Otherwise, consult Accept
    # header.
    if format is not None:
        media_types_or_aliases = format.split(",")
        # Resolve aliases, like "csv" -> "text/csv".
        media_types = [types_map.get("." + t, t) for t in media_types_or_aliases]
    else:
        # The HTTP spec says these should be separated by ", " but some
        # browsers separate with just "," (no space).
        # https://developer.mozilla.org/en-US/docs/Web/HTTP/Content_negotiation/List_of_default_Accept_values#default_values  # noqa
        # That variation is what we are handling below with lstrip.
        media_types = [
            s.lstrip(" ")
            for s in request_headers.get("Accept", DEFAULT_MEDIA_TYPE).split(",")
        ]
    # The client may give us a choice of media types. Find the first one
    # that we support.
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


APACHE_ARROW_FILE_MIME_TYPE = "vnd.apache.arrow.file"


def construct_dataframe_response(df, request_headers, format=None):
    etag = dask.base.tokenize(df)
    if request_headers.get("If-None-Match", "") == etag:
        return Response(status_code=304)
    # Give priority to the `format` query parameter. Otherwise, consult Accept
    # header.
    if format is not None:
        media_types_or_aliases = format.split(",")
        # Resolve aliases, like "csv" -> "text/csv".
        media_types = [types_map.get("." + t, t) for t in media_types_or_aliases]
    else:
        # The HTTP spec says these should be separated by ", " but some
        # browsers separate with just "," (no space).
        # https://developer.mozilla.org/en-US/docs/Web/HTTP/Content_negotiation/List_of_default_Accept_values#default_values  # noqa
        # That variation is what we are handling below with lstrip.
        media_types = [
            s.lstrip(" ")
            for s in request_headers.get("Accept", APACHE_ARROW_FILE_MIME_TYPE).split(
                ","
            )
        ]
    # The client may give us a choice of media types. Find the first one
    # that we support.
    for media_type in media_types:
        if media_type == "*/*":
            media_type = APACHE_ARROW_FILE_MIME_TYPE
        if media_type in serialization_registry.media_types("dataframe"):
            content = serialization_registry("dataframe", media_type, df)
            return PatchedResponse(
                content=content, media_type=media_type, headers={"ETag": etag}
            )
    else:
        raise UnsupportedMediaTypes(
            "None of the media types requested by the client are supported.",
            unsupported=media_types,
            supported=serialization_registry.media_types("dataframe"),
        )


def construct_dataset_response(dataset, request_headers, format=None):
    DEFAULT_MEDIA_TYPE = "application/netcdf"
    etag = dask.base.tokenize(dataset)
    if request_headers.get("If-None-Match", "") == etag:
        return Response(status_code=304)
    # Give priority to the `format` query parameter. Otherwise, consult Accept
    # header.
    if format is not None:
        media_types_or_aliases = format.split(",")
        # Resolve aliases, like "csv" -> "text/csv".
        media_types = [types_map.get("." + t, t) for t in media_types_or_aliases]
    else:
        # The HTTP spec says these should be separated by ", " but some
        # browsers separate with just "," (no space).
        # https://developer.mozilla.org/en-US/docs/Web/HTTP/Content_negotiation/List_of_default_Accept_values#default_values  # noqa
        # That variation is what we are handling below with lstrip.
        media_types = [
            s.lstrip(" ")
            for s in request_headers.get("Accept", DEFAULT_MEDIA_TYPE).split(",")
        ]
    # The client may give us a choice of media types. Find the first one
    # that we support.
    for media_type in media_types:
        if media_type == "*/*":
            media_type = APACHE_ARROW_FILE_MIME_TYPE
        if media_type in serialization_registry.media_types("dataset"):
            content = serialization_registry("dataset", media_type, dataset)
            return PatchedResponse(
                content=content, media_type=media_type, headers={"ETag": etag}
            )
    else:
        raise UnsupportedMediaTypes(
            "None of the media types requested by the client are supported.",
            unsupported=media_types,
            supported=serialization_registry.media_types("dataset"),
        )


def construct_resource(path, key, entry, fields):
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
        structure = {}
        if models.EntryFields.structure_family in fields:
            attributes["structure_family"] = entry.structure_family
        if models.EntryFields.macrostructure in fields:
            macrostructure = entry.macrostructure()
            if macrostructure is not None:
                structure["macro"] = dataclasses.asdict(macrostructure)
        if models.EntryFields.microstructure in fields:
            if entry.structure_family == "dataframe":
                # Special case: its microstructure is cannot be JSON-serialized
                # and is therefore available from separate routes. Sends links
                # instead of the actual payload.
                structure["micro"] = {
                    "links": {
                        "meta": f"/dataframe/meta/{path}",
                        "divisions": f"/dataframe/divisions/{path}",
                    }
                }
            else:
                microstructure = entry.microstructure()
                if microstructure is not None:
                    structure["micro"] = dataclasses.asdict(microstructure)
        attributes["structure"] = structure
        resource = models.ReaderResource(
            **{
                "id": key,
                "attributes": models.ReaderAttributes(**attributes),
                "type": models.EntryType.reader,
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
    content_as_dict = content.dict()
    content_hash = md5(str(content_as_dict).encode()).hexdigest()
    headers = {"ETag": content_hash}
    for media_type in media_types:
        if media_type == "*/*":
            media_type = DEFAULT_MEDIA_TYPE
        if media_type == "application/x-msgpack":
            return MsgpackResponse(content_as_dict, headers=headers)
        if media_type == "application/json":
            return JSONResponse(content_as_dict, headers=headers)
    else:
        # It is commmon in HTTP to fall back on a default representation if
        # none of the requested ones are avaiable. We do not do this for
        # data payloads, but it makes some sense to do it for these metadata
        # messages.
        return JSONResponse(content_as_dict, headers=headers)


class UnsupportedMediaTypes(Exception):
    def __init__(self, message, unsupported, supported):
        self.unsupported = unsupported
        self.supported = supported
        super().__init__(message)


class NoEntry(KeyError):
    pass


class WrongTypeForRoute(Exception):
    pass
