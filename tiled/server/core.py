import base64
import collections.abc
import dataclasses
import itertools
import math
import operator
import re
import sys
import uuid
from collections import defaultdict
from datetime import datetime, timedelta
from hashlib import md5
from typing import Any

import dateutil.tz
import jmespath
import msgpack
from fastapi import HTTPException, Response
from starlette.responses import JSONResponse, Send, StreamingResponse

# Some are not directly used, but they register things on import.
from .. import queries
from ..adapters.mapping import MapAdapter
from ..queries import KeyLookup, QueryValueError
from ..serialization import register_builtin_serializers
from ..utils import (
    APACHE_ARROW_FILE_MIME_TYPE,
    SerializationError,
    UnsupportedShape,
    safe_json_dump,
)
from . import schemas
from .etag import tokenize
from .utils import record_timing

del queries
register_builtin_serializers()


_FILTER_PARAM_PATTERN = re.compile(r"filter___(?P<name>.*)___(?P<field>[^\d\W][\w\d]+)")
_LOCAL_TZINFO = dateutil.tz.gettz()

# Pragmatic limit on how "wide" a node can be before the server refuses to
# inline its contents.
# There are some wide-and-short DataFrame datasets we have observed
# (specifically, bluesky "baseline" readings) that can touch this limit.
# We raised it from an initial guess of 100 to avoid splitting these over
# too many requests. This value *may* be too high --- need more benchmarking
# on realistic workloads.
INLINED_CONTENTS_LIMIT = 500

# Pragmatic limit on how deep the server will recurse into nodes that request
# inlined contents. This is a hard upper bound meant to protect the server from
# being crashed by badly designed or buggy Adapters. It is up to Adapters to
# opt in to this behavior and decide on a reasonable depth.
DEPTH_LIMIT = 5


def len_or_approx(tree):
    """
    Prefer approximate length if implemented. (It's cheaper.)
    """
    try:
        return operator.length_hint(tree)
    except TypeError:
        return len(tree)


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


def construct_entries_response(
    query_registry,
    tree,
    route,
    path,
    offset,
    limit,
    fields,
    select_metadata,
    omit_links,
    filters,
    sort,
    base_url,
    media_type,
    max_depth,
):
    path_parts = [segment for segment in path.split("/") if segment]
    if tree.structure_family != "node":
        raise WrongTypeForRoute("This is not a Node; it does not have entries.")
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
        if not hasattr(tree, "sort"):
            raise HTTPException(
                status_code=400, detail="This Tree does not support sorting."
            )
        tree = tree.sort(sorting)
    # Apply the queries and obtain a narrowed tree.
    key_lookups = []
    for query_name, parameters_dict_of_lists in queries.items():
        for i in itertools.count(0):
            try:
                parameters = {
                    field_name: parameters_list[i]
                    for field_name, parameters_list in parameters_dict_of_lists.items()
                }
            except IndexError:
                break
            query_class = query_registry.name_to_query_type[query_name]
            try:
                query = query_class.decode(**parameters)
                # Special case: Do key-lookups at the end after all other filtering.
                # We do not require trees to implement this query; we implement it
                # directly here by just calling __getitem__.
                if isinstance(query, KeyLookup):
                    key_lookups.append(query.key)
                    continue
                tree = tree.search(query)
            except QueryValueError as err:
                raise HTTPException(status_code=400, detail=err.args[0])
    if key_lookups:
        # Duplicates are technically legal because *any* query can be given
        # with multiple parameters.
        unique_key_lookups = set(key_lookups)
        (key_lookup), *others = unique_key_lookups
        if others:
            # Two non-equal KeyLookup queries must return no results.
            tree = MapAdapter({})
        else:
            try:
                tree = MapAdapter({key_lookup: tree[key_lookup]}, must_revalidate=False)
            except KeyError:
                tree = MapAdapter({})
    count = len_or_approx(tree)
    links = pagination_links(route, path_parts, offset, limit, count)
    data = []
    if fields != [schemas.EntryFields.none]:
        # Pull a page of items into memory.
        items = tree.items()[offset : offset + limit]  # noqa: E203
    else:
        # Pull a page of just the keys, which is cheaper.
        items = (
            (key, None) for key in tree.keys()[offset : offset + limit]  # noqa: E203
        )
    # This value will not leak out. It just used to seed comparisons.
    metadata_stale_at = datetime.utcnow() + timedelta(days=1_000_000)
    must_revalidate = getattr(tree, "must_revalidate", True)
    for key, entry in items:
        resource = construct_resource(
            base_url,
            path_parts + [key],
            entry,
            fields,
            select_metadata,
            omit_links,
            media_type,
            max_depth=max_depth,
        )
        data.append(resource)
        # If any entry has emtry.metadata_stale_at = None, then there will
        # be no 'Expires' header. We will pessimistically assume the values
        # are immediately stale.
        if metadata_stale_at is not None:
            if getattr(entry, "metadata_stale_at", None) is None:
                metadata_stale_at = None
            else:
                metadata_stale_at = min(metadata_stale_at, entry.metadata_stale_at)
    return (
        schemas.Response(data=data, links=links, meta={"count": count}),
        metadata_stale_at,
        must_revalidate,
    )


DEFAULT_MEDIA_TYPES = {
    "array": {"*/*": "application/octet-stream", "image/*": "image/png"},
    "dataframe": {"*/*": APACHE_ARROW_FILE_MIME_TYPE},
    "node": {"*/*": "application/x-hdf5"},
    "sparse": {"*/*": APACHE_ARROW_FILE_MIME_TYPE},
}


def construct_revisions_response(
    entry,
    route,
    path,
    offset,
    limit,
    media_type,
):
    path_parts = [segment for segment in path.split("/") if segment]
    revisions = entry.revisions[offset : offset + limit]  # noqa: E203
    data = []
    for revision in revisions:
        item = {
            "revision": revision["revision"],
            "attributes": {
                "metadata": revision["metadata"],
                "specs": revision["specs"],
                "updated_at": revision["updated_at"],
            },
        }
        data.append(item)
    count = len(data)
    links = pagination_links(
        route, path_parts, offset, limit, count
    )  # maybe reuse or maybe make a new pagination_revision_links
    return schemas.Response(data=data, links=links, meta={"count": count})


def construct_data_response(
    structure_family,
    serialization_registry,
    payload,
    metadata,
    request,
    format=None,
    specs=None,
    expires=None,
    filename=None,
):
    request.state.endpoint = "data"
    if specs is None:
        specs = []
    default_media_type = DEFAULT_MEDIA_TYPES[structure_family]["*/*"]
    # Give priority to the `format` query parameter. Otherwise, consult Accept
    # header.
    if format is not None:
        media_types_or_aliases = format.split(",")
        # Resolve aliases, like "csv" -> "text/csv".
        media_types = [
            serialization_registry.resolve_alias(t) for t in media_types_or_aliases
        ]
    else:
        # The HTTP spec says these should be separated by ", " but some
        # browsers separate with just "," (no space).
        # https://developer.mozilla.org/en-US/docs/Web/HTTP/Content_negotiation/List_of_default_Accept_values#default_values  # noqa
        # That variation is what we are handling below with lstrip.
        media_types = [
            s.lstrip(" ")
            for s in request.headers.get("Accept", default_media_type).split(",")
        ]

    # The client may give us a choice of media types. Find the first one
    # that we support.
    supported = set()
    for media_type in media_types:
        if media_type == "*/*":
            media_type = DEFAULT_MEDIA_TYPES[structure_family]["*/*"]
        elif structure_family == "array" and media_type == "image/*":
            media_type = DEFAULT_MEDIA_TYPES[structure_family]["image/*"]
        # Compare the request formats to the formats supported by each spec
        # and, finally, by the structure family.
        for spec in specs + [structure_family]:
            media_types_for_spec = serialization_registry.media_types(spec)
            if media_type in media_types_for_spec:
                break
            supported.update(media_types_for_spec)
        else:
            # None of the specs or the structure_family can serialize to this
            # media_type. Try the next one.
            continue
        # We found a match above. We have our media_type.
        break
    else:
        # We have checked each of the media_types, and we cannot serialize
        # to any of them.
        raise UnsupportedMediaTypes(
            f"None of the media types requested by the client are supported. "
            f"Supported: {', '.join(supported)}. Requested: {', '.join(media_types)}.",
        )
    with record_timing(request.state.metrics, "tok"):
        # Create an ETag that uniquely identifies this content and the media
        # type that it will be encoded as.
        etag = tokenize((payload, media_type))
    headers = {"ETag": etag}
    if expires is not None:
        headers["Expires"] = expires.strftime(HTTP_EXPIRES_HEADER_FORMAT)
    if request.headers.get("If-None-Match", "") == etag:
        # If the client already has this content, confirm that.
        return Response(status_code=304, headers=headers)
    if filename:
        headers["Content-Disposition"] = f"attachment;filename={filename}"
    # This is the expensive step: actually serialize.
    try:
        content = serialization_registry(spec, media_type, payload, metadata)
    except UnsupportedShape as err:
        raise UnsupportedMediaTypes(
            f"The shape of this data {err.args[0]} is incompatible with the requested format ({media_type}). "
            f"Slice it or choose a different format.",
        )
    except SerializationError as err:
        raise UnsupportedMediaTypes(
            f"This type is supported in general but there was an error packing this specific data: {err.args[0]}",
        )
    return PatchedResponse(
        content=content,
        media_type=media_type,
        headers=headers,
    )


def construct_resource(
    base_url,
    path_parts,
    entry,
    fields,
    select_metadata,
    omit_links,
    media_type,
    max_depth,
    depth=0,
):
    path_str = "/".join(path_parts)
    attributes = {"ancestors": path_parts[:-1]}
    if schemas.EntryFields.metadata in fields:
        if select_metadata is not None:
            attributes["metadata"] = jmespath.compile(select_metadata).search(
                entry.metadata
            )
        else:
            attributes["metadata"] = entry.metadata
    if schemas.EntryFields.specs in fields:
        attributes["specs"] = getattr(entry, "specs", [])
    if (entry is not None) and entry.structure_family == "node":
        attributes["structure_family"] = "node"
        if schemas.EntryFields.structure in fields:
            if (
                ((max_depth is None) or (depth < max_depth))
                and hasattr(entry, "inlined_contents_enabled")
                and entry.inlined_contents_enabled(depth)
                and depth <= DEPTH_LIMIT
            ):
                # This node wants us to inline its contents.
                # First check that it is not too large.
                est_count = len_or_approx(entry)
                if est_count > INLINED_CONTENTS_LIMIT:
                    # Too large: do not inline its contents.
                    count = est_count
                    contents = None
                else:
                    contents = {}
                    # The size may change as we are walking the entry.
                    # Keep a *true* count separately from est_count.
                    count = 0
                    for key, adapter in entry.items():
                        count += 1
                        if count > INLINED_CONTENTS_LIMIT:
                            # The est_count was inaccurate or else the entry has grown
                            # new children while we are walking it. Too large!
                            count = len_or_approx(entry)
                            contents = None
                            break
                        contents[key] = construct_resource(
                            base_url,
                            path_parts + [key],
                            adapter,
                            fields,
                            select_metadata,
                            omit_links,
                            media_type,
                            max_depth,
                            depth=1 + depth,
                        )
            else:
                count = len_or_approx(entry)
                contents = None
            structure = schemas.NodeStructure(
                count=count,
                contents=contents,
            )
            attributes["structure"] = structure
        if schemas.EntryFields.sorting in fields:
            if hasattr(entry, "sorting"):
                # In the Python API we encode sorting as (key, direction).
                # This order-based "record" notion does not play well with OpenAPI.
                # In the HTTP API, therefore, we use {"key": key, "direction": direction}.
                attributes["sorting"] = [
                    {"key": key, "direction": direction}
                    for key, direction in entry.sorting
                ]
        d = {
            "id": path_parts[-1] if path_parts else "",
            "attributes": schemas.NodeAttributes(**attributes),
        }
        if not omit_links:
            d["links"] = {
                "self": f"{base_url}/node/metadata/{path_str}",
                "search": f"{base_url}/node/search/{path_str}",
                "full": f"{base_url}/node/full/{path_str}",
            }
        resource = schemas.Resource[
            schemas.NodeAttributes, schemas.NodeLinks, schemas.NodeMeta
        ](**d)
    else:
        links = {"self": f"{base_url}/node/metadata/{path_str}"}
        structure = {}
        if entry is not None:
            # entry is None when we are pulling just *keys* from the
            # Tree and not values.
            ResourceLinksT = schemas.resource_links_type_by_structure_family[
                entry.structure_family
            ]
            links.update(
                {
                    link: template.format(base_url=base_url, path=path_str)
                    for link, template in FULL_LINKS[entry.structure_family].items()
                }
            )
            if schemas.EntryFields.structure_family in fields:
                attributes["structure_family"] = entry.structure_family
            if entry.structure_family == "sparse":
                if schemas.EntryFields.structure in fields:
                    structure = dataclasses.asdict(entry.structure())
                    shape = structure.get("shape")
                else:
                    # The client did not request structure so we have not yet
                    # accessed it, and we have access it specifically to construct this link.
                    shape = entry.structure().shape
                    structure = None
                block_template = ",".join(f"{{{index}}}" for index in range(len(shape)))
                links[
                    "block"
                ] = f"{base_url}/array/block/{path_str}?block={block_template}"
            else:
                if (schemas.EntryFields.macrostructure in fields) or (
                    schemas.EntryFields.structure in fields
                ):
                    macrostructure = entry.macrostructure()
                    if macrostructure is not None:
                        structure["macro"] = dataclasses.asdict(macrostructure)
                if (schemas.EntryFields.microstructure in fields) or (
                    schemas.EntryFields.structure in fields
                ):
                    if entry.structure_family == "node":
                        assert False  # not sure if this ever happens
                        pass
                    elif entry.structure_family == "dataframe":
                        microstructure = entry.microstructure()
                        meta = microstructure.meta
                        divisions = microstructure.divisions
                        if media_type == "application/json":
                            # For JSON, base64-encode the binary Arrow-encoded data,
                            # and indicate that this has been done in the data URI.
                            data_uri = f"data:{APACHE_ARROW_FILE_MIME_TYPE};base64,"
                            meta = data_uri + base64.b64encode(meta).decode()
                            divisions = data_uri + base64.b64encode(divisions).decode()
                        else:
                            # In msgpack, we can encode the binary Arrow-encoded data directly.
                            assert media_type == "application/x-msgpack"
                        structure["micro"] = {
                            "meta": meta,
                            "divisions": divisions,
                        }
                    else:
                        microstructure = entry.microstructure()
                        if microstructure is not None:
                            structure["micro"] = dataclasses.asdict(microstructure)
                if entry.structure_family == "array":
                    shape = structure.get("macro", {}).get("shape")
                    if shape is None:
                        # The client did not request structure so we have not yet
                        # accessed it, and we have access it specifically to construct this link.
                        shape = entry.macrostructure().shape
                    block_template = ",".join(
                        f"{{index_{index}}}" for index in range(len(shape))
                    )
                    links[
                        "block"
                    ] = f"{base_url}/array/block/{path_str}?block={block_template}"
                elif entry.structure_family == "dataframe":
                    links[
                        "partition"
                    ] = f"{base_url}/dataframe/partition/{path_str}?partition={{index}}"
            attributes["structure"] = structure
        else:
            # We only have entry names, not structure_family, so
            ResourceLinksT = schemas.SelfLinkOnly
        d = {
            "id": path_parts[-1],
            "attributes": schemas.NodeAttributes(**attributes),
        }
        if not omit_links:
            d["links"] = links
        resource = schemas.Resource[
            schemas.NodeAttributes, ResourceLinksT, schemas.EmptyDict
        ](**d)
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


class NumpySafeJSONResponse(JSONResponse):
    def __init__(self, *args, metrics, **kwargs):
        self.__metrics = metrics
        super().__init__(*args, **kwargs)

    def render(self, content: Any) -> bytes:
        with record_timing(self.__metrics, "pack"):
            return safe_json_dump(content)


def _fallback_msgpack_encoder(obj):
    # If numpy has not been imported yet, then we can be sure that obj
    # is not a numpy object, and we want to avoid triggering a numpy
    # import. (The server does not have a hard numpy dependency.)
    if "numpy" in sys.modules:
        import numpy

        if isinstance(obj, (numpy.generic, numpy.ndarray)):
            if numpy.isscalar(obj):
                return obj.item()
            return obj.tolist()
    if isinstance(obj, uuid.UUID):
        return str(obj)  # hyphen-separated hex per RFC4122
    return obj


def _patch_naive_datetimes(obj):
    """
    If a naive datetime is found, attach local time.

    Msgpack can only serialize datetimes with tzinfo.
    """
    if hasattr(obj, "items"):
        patched_obj = {}
        for k, v in obj.items():
            patched_obj[k] = _patch_naive_datetimes(v)
    elif (not isinstance(obj, str)) and isinstance(obj, collections.abc.Iterable):
        patched_obj = []
        for item in obj:
            patched_obj.append(_patch_naive_datetimes(item))
    elif isinstance(obj, datetime) and obj.tzinfo is None:
        patched_obj = obj.astimezone(_LOCAL_TZINFO)
    else:
        patched_obj = obj
    return patched_obj


class MsgpackResponse(Response):
    media_type = "application/x-msgpack"

    def __init__(self, *args, metrics, **kwargs):
        self.__metrics = metrics
        super().__init__(*args, **kwargs)

    def render(self, content: Any, _reentered=False) -> bytes:
        try:
            with record_timing(self.__metrics, "pack"):
                return msgpack.packb(
                    content, default=_fallback_msgpack_encoder, datetime=True
                )
        except (ValueError, TypeError) as err:
            # msgpack tries to handle all datetimes, but if it
            # received a naive one (tzinfo=None) then it fails.
            # We cannot use the default hook to handle this because
            # it is not called.
            if "can not serialize 'datetime.datetime' object" in str(err) and (
                not _reentered
            ):
                patched_content = _patch_naive_datetimes(content)
                return self.render(patched_content, _reentered=True)
            raise


JSON_MIME_TYPE = "application/json"
MSGPACK_MIME_TYPE = "application/x-msgpack"
# This is a silly time format, but it is the HTTP standard.
HTTP_EXPIRES_HEADER_FORMAT = "%a, %d %b %Y %H:%M:%S GMT"


def resolve_media_type(request):
    media_types = request.headers.get("Accept", JSON_MIME_TYPE).split(", ")
    for media_type in media_types:
        if media_type == "*/*":
            media_type = JSON_MIME_TYPE
            break
        if media_type == MSGPACK_MIME_TYPE:
            break
        if media_type == JSON_MIME_TYPE:
            break
    else:
        # It is commmon in HTTP to fall back on a default representation if
        # none of the requested ones are available. We do not do this for
        # data payloads, but it makes some sense to do it for these metadata
        # messages.
        media_type = JSON_MIME_TYPE
    assert media_type in {JSON_MIME_TYPE, MSGPACK_MIME_TYPE}
    return media_type


def json_or_msgpack(request, content, expires=None, headers=None):
    media_type = resolve_media_type(request)
    with record_timing(request.state.metrics, "tok"):
        etag = md5(str(content).encode()).hexdigest()
    headers = headers or {}
    headers["ETag"] = etag
    if expires is not None:
        headers["Expires"] = expires.strftime(HTTP_EXPIRES_HEADER_FORMAT)
    if request.headers.get("If-None-Match", "") == etag:
        # If the client already has this content, confirm that.
        return Response(status_code=304, headers=headers)
    if media_type == "application/x-msgpack":
        return MsgpackResponse(content, headers=headers, metrics=request.state.metrics)
    return NumpySafeJSONResponse(
        content, headers=headers, metrics=request.state.metrics
    )


class UnsupportedMediaTypes(Exception):
    pass


class NoEntry(KeyError):
    pass


class WrongTypeForRoute(Exception):
    pass


FULL_LINKS = {
    "node": {"full": "{base_url}/node/full/{path}"},
    "array": {"full": "{base_url}/array/full/{path}"},
    "dataframe": {"full": "{base_url}/node/full/{path}"},
    "sparse": {"full": "{base_url}/array/full/{path}"},
}
