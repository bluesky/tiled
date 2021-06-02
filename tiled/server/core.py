import abc
from collections import defaultdict
import dataclasses
from hashlib import md5
import itertools
import json
import math
from mimetypes import types_map
import operator
import re
import sys
from typing import Any, Optional

import dask.base
from fastapi import Depends, HTTPException, Query, Response
import msgpack
import pydantic
from starlette.responses import JSONResponse, StreamingResponse, Send

from . import models
from .authentication import get_current_user
from ..utils import modules_available
from ..query_registration import name_to_query_type
from ..queries import QueryValueError
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


def get_root_catalog():
    raise NotImplementedError(
        "This should be overridden via dependency_overrides. "
        "See tiled.server.app.serve_catalog()."
    )


def entry(
    path: str,
    current_user: str = Depends(get_current_user),
    root_catalog: pydantic.BaseSettings = Depends(get_root_catalog),
):
    path_parts = [segment for segment in path.split("/") if segment]
    entry = root_catalog.authenticated_as(current_user)
    try:
        # Traverse into sub-catalog(s).
        for segment in path_parts:
            try:
                entry = entry[segment]
            except (KeyError, TypeError):
                raise NoEntry(path_parts)
        return entry
    except NoEntry:
        raise HTTPException(status_code=404, detail=f"No such entry: {path_parts}")


def reader(
    entry: Any = Depends(entry),
):
    "Specify a path parameter and use it to look up a reader."
    if not isinstance(entry, DuckReader):
        raise HTTPException(status_code=404, detail="This is not a Reader.")
    return entry


def block(
    # Ellipsis as the "default" tells FastAPI to make this parameter required.
    block: str = Query(..., min_length=1, regex="^[0-9]+(,[0-9]+)*$"),
):
    "Specify and parse a block index parameter."
    return tuple(map(int, block.split(",")))


def expected_shape(
    expected_shape: Optional[str] = Query(
        None, min_length=1, regex="^[0-9]+(,[0-9]+)*$"
    ),
):
    "Specify and parse an expected_shape parameter."
    if expected_shape is None:
        return
    return tuple(map(int, expected_shape.split(",")))


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
    """
    Prefer approximate length if implemented. (It's cheaper.)
    """
    try:
        return operator.length_hint(catalog)
    except TypeError:
        return len(catalog)


def pagination_links(route, path_parts, offset, limit, length_hint):
    path_str = "/".join(path_parts)
    links = {
        "self": f"{route}/{path_str}?page[offset]={offset}&page[limit]={limit}",
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
                "first": f"{route}/{path_str}?page[offset]={0}&page[limit]={limit}",
                "last": f"{route}/{path_str}?page[offset]={last_page}&page[limit]={limit}",
            }
        )
    if offset + limit < length_hint:
        links[
            "next"
        ] = f"{route}/{path_str}?page[offset]={offset + limit}&page[limit]={limit}"
    if offset > 0:
        links[
            "prev"
        ] = f"{route}/{path_str}?page[offset]={max(0, offset - limit)}&page[limit]={limit}"
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
    sort,
    base_url,
):
    path_parts = [segment for segment in path.split("/") if segment]
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
    sorting = []
    if sort is not None:
        for item in sort.split(","):
            if item:
                if item.startswith("-"):
                    sorting.append((item[1:], -1))
                else:
                    sorting.append((item, 1))
    if sorting:
        if not hasattr(catalog, "sort"):
            raise HTTPException(
                status_code=400, detail="This Catalog does not support sorting."
            )
        catalog = catalog.sort(sorting)
    # Apply the queries and obtain a narrowed catalog.
    for query_name, parameters_dict_of_lists in queries.items():
        for i in itertools.count(0):
            try:
                parameters = {
                    field_name: parameters_list[i]
                    for field_name, parameters_list in parameters_dict_of_lists.items()
                }
            except IndexError:
                break
            query_class = name_to_query_type[query_name]
            # Special case:
            # List fields are serialized as comma-separated strings.
            for field in dataclasses.fields(query_class):
                if getattr(field.type, "__origin__", None) is list:
                    (inner_type,) = field.type.__args__
                    parameters[field.name] = [
                        inner_type(item) for item in parameters[field.name].split(",")
                    ]
            try:
                query = query_class(**parameters)
                catalog = catalog.search(query)
            except QueryValueError as err:
                raise HTTPException(status_code=400, detail=err.args[0])
    count = len_or_approx(catalog)
    links = pagination_links(route, path_parts, offset, limit, count)
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
        resource = construct_resource(base_url, path_parts + [key], entry, fields)
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


def construct_resource(base_url, path_parts, entry, fields):
    path_str = "/".join(path_parts)
    attributes = {}
    if models.EntryFields.metadata in fields:
        attributes["metadata"] = entry.metadata
    if models.EntryFields.client_type_hint in fields:
        attributes["client_type_hint"] = getattr(entry, "client_type_hint", None)
    if isinstance(entry, DuckCatalog):
        if models.EntryFields.count in fields:
            attributes["count"] = len_or_approx(entry)
            if hasattr(entry, "sorting"):
                attributes["sorting"] = entry.sorting
        resource = models.CatalogResource(
            **{
                "id": path_parts[-1] if path_parts else "",
                "attributes": models.CatalogAttributes(**attributes),
                "type": models.EntryType.catalog,
                "links": {
                    "self": f"{base_url}metadata/{path_str}",
                    "search": f"{base_url}search/{path_str}",
                },
            }
        )
    else:
        links = {"self": f"{base_url}metadata/{path_str}"}
        structure = {}
        if entry is not None:
            # entry is None when we are pulling just *keys* from the
            # Catalog and not values.
            links.update(
                {
                    link: template.format(base_url=base_url, path=path_str)
                    for link, template in FULL_LINKS[entry.structure_family].items()
                }
            )
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
                            "meta": f"{base_url}dataframe/meta/{path_str}",
                            "divisions": f"{base_url}dataframe/divisions/{path_str}",
                        }
                    }
                else:
                    microstructure = entry.microstructure()
                    if microstructure is not None:
                        structure["micro"] = dataclasses.asdict(microstructure)
                if entry.structure_family == "array":
                    block_template = ",".join(
                        f"{{index_{index}}}"
                        for index in range(len(structure["macro"]["shape"]))
                    )
                    links[
                        "block"
                    ] = f"{base_url}array/block/{path_str}?block={block_template}"
                elif entry.structure_family == "dataframe":
                    links[
                        "partition"
                    ] = f"{base_url}dataframe/partition/{path_str}?partition={{index}}"
                elif entry.structure_family == "variable":
                    block_template = ",".join(
                        f"{{index_{index}}}"
                        for index in range(
                            len(structure["macro"]["data"]["macro"]["shape"])
                        )
                    )
                    links[
                        "block"
                    ] = f"{base_url}variable/block/{path_str}?block={block_template}"
                elif entry.structure_family == "data_array":
                    block_template = ",".join(
                        f"{{index_{index}}}"
                        for index in range(
                            len(structure["macro"]["variable"]["macro"]["data"])
                        )
                    )
                    links[
                        "block"
                    ] = f"{base_url}data_array/block/{path_str}?block={block_template}"
                elif entry.structure_family == "dataset":
                    links[
                        "block"
                    ] = f"{base_url}dataset/block/{path_str}?variable={{variable}}&block={{block_indexes}}"
                    microstructure = entry.microstructure()
            attributes["structure"] = structure
        resource = models.ReaderResource(
            **{
                "id": path_parts[-1],
                "attributes": models.ReaderAttributes(**attributes),
                "type": models.EntryType.reader,
                "links": links,
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


class _NumpySafeJSONEncoder(json.JSONEncoder):
    """
    A json.JSONEncoder for encoding numpy objects using built-in Python types.

    Examples
    --------

    Encode a Python object that includes an arbitrarily-nested numpy object.

    >>> json.dumps({'a': {'b': numpy.array([1, 2, 3])}}, cls=NumpyEncoder)
    """

    def default(self, obj):
        # JSON cannot represent the unicode/bytes distinction, so we send str.
        # Msgpack *does* understand this distinction so clients can use that
        # format if they care about the distinction.
        if isinstance(obj, bytes):
            return obj.decode()
        # If numpy has not been imported yet, then we can be sure that obj
        # is not a numpy object, and we want to avoid triggering a numpy
        # import. (The server does not have a hard numpy dependency.)
        if "numpy" in sys.modules:
            import numpy

            if isinstance(obj, (numpy.generic, numpy.ndarray)):
                if numpy.isscalar(obj):
                    return obj.item()
                return obj.tolist()
        return super().default(obj)


class NumpySafeJSONResponse(JSONResponse):
    def render(self, content: Any) -> bytes:
        # Try the built-in rendering. If it fails, do the more
        # expensive walk to convert any bytes objects to unicode
        # and any numpy objects to builtins.
        try:
            # Fast (optimistic) path
            return super().render(content)
        except Exception:
            return json.dumps(content, cls=_NumpySafeJSONEncoder).encode()


def _numpy_safe_msgpack_encoder(obj):
    # If numpy has not been imported yet, then we can be sure that obj
    # is not a numpy object, and we want to avoid triggering a numpy
    # import. (The server does not have a hard numpy dependency.)
    if "numpy" in sys.modules:
        import numpy

        if isinstance(obj, (numpy.generic, numpy.ndarray)):
            if numpy.isscalar(obj):
                return obj.item()
            return obj.tolist()
    return obj


class MsgpackResponse(Response):
    media_type = "application/x-msgpack"

    def render(self, content: Any) -> bytes:
        return msgpack.packb(content, default=_numpy_safe_msgpack_encoder)


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
            return NumpySafeJSONResponse(content_as_dict, headers=headers)
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


FULL_LINKS = {
    "array": {"full": "{base_url}array/full/{path}"},
    "dataframe": {"full": "{base_url}dataframe/full/{path}"},
    "variable": {"full": "{base_url}variable/full/{path}"},
    "data_array": {"full_variable": "{base_url}data_array/variable/full/{path}"},
    "dataset": {
        "full_variable": "{base_url}dataset/data_var/full/{path}?variable={{variable}}",
        "full_coordinate": "{base_url}dataset/coord/full/{path}?variable={{variable}}",
        "full_dataset": "{base_url}dataset/full/{path}",
    },
}
