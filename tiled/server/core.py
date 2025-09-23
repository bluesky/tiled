import collections.abc
import dataclasses
import inspect
import itertools
import math
import operator
import re
import sys
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from hashlib import md5
from typing import Any, Optional

import anyio
import dateutil.tz
import jmespath
import msgpack
from fastapi import HTTPException, Response, WebSocket
from starlette.responses import JSONResponse, StreamingResponse
from starlette.status import HTTP_200_OK, HTTP_304_NOT_MODIFIED, HTTP_400_BAD_REQUEST

# Some are not directly used, but they register things on import.
from .. import queries
from ..adapters.mapping import MapAdapter
from ..queries import KeyLookup, QueryValueError
from ..serialization import register_builtin_serializers
from ..structures.core import Spec, StructureFamily
from ..utils import (
    APACHE_ARROW_FILE_MIME_TYPE,
    BrokenLink,
    SerializationError,
    UnsupportedShape,
    ensure_awaitable,
    parse_mimetype,
    safe_json_dump,
)
from . import schemas
from .etag import tokenize
from .links import links_for_node
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

DEFAULT_PAGE_SIZE = 100
MAX_PAGE_SIZE = 300


async def len_or_approx(tree, exact=False, threshold=5000):
    """Calculate the length of a tree, either exactly or approximately

    Prefer approximate length if implemented. (It's cheaper.)

    Parameters
    ----------
    tree : Tree
        The tree to calculate the length of.
    exact : bool, optional
        If True, always return the exact length. If False, return an
        approximate length if available.
    threshold : int
        If the exact length is less than this threshold, return it;
        otherwise, return an approximate length of this lower bound.
        If set to -1, return the exact length.
        Only used if `exact` is False.

    Returns
    -------
    int
        The length of the tree, either exact or approximate.
    """

    # Override the exact flag if threshold is set to -1
    exact = exact or (threshold == -1)

    # First, try to get a lower bound on the length
    lbound = None
    if hasattr(tree, "lbound_len") and not exact:
        lbound = await tree.lbound_len(threshold=threshold)
        if lbound <= threshold:
            # This is the exact length
            return lbound

    # Try approximate count if the lower bound is above the threshold or None
    if hasattr(tree, "approx_len") and not exact:
        approx = await tree.approx_len()
        if approx is not None:
            return approx
    if lbound is not None:
        return lbound  # If we have a lower bound, return it

    # If we have neither, fall back to the exact length
    if hasattr(tree, "exact_len"):
        return await tree.exact_len()

    # If the tree does not implement any of these, use the sync length (or hint)
    try:
        if not exact:
            return await anyio.to_thread.run_sync(operator.length_hint, tree)
    except TypeError:
        pass
    finally:
        return await anyio.to_thread.run_sync(len, tree)


def pagination_links(base_url, route, path_parts, offset, limit, length_hint):
    path_str = "/".join(path_parts)
    links = {
        "self": f"{base_url}{route}/{path_str}?page[offset]={offset}&page[limit]={limit}",
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
                "first": f"{base_url}{route}/{path_str}?page[offset]={0}&page[limit]={limit}",
                "last": f"{base_url}{route}/{path_str}?page[offset]={last_page}&page[limit]={limit}",
            }
        )
    if offset + limit < length_hint:
        links[
            "next"
        ] = f"{base_url}{route}/{path_str}?page[offset]={offset + limit}&page[limit]={limit}"
    if offset > 0:
        links[
            "prev"
        ] = f"{base_url}{route}/{path_str}?page[offset]={max(0, offset - limit)}&page[limit]={limit}"
    return links


async def apply_search(tree, filters, query_registry):
    queries = defaultdict(
        dict
    )  # e.g. {"text": {"text": "dog"}, "lookup": {"key": "..."}}
    # Group the parameters by query type.
    for key, value in filters.items():
        if value is None:
            continue
        name, field = _FILTER_PARAM_PATTERN.match(key).groups()
        queries[name][field] = value

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
                raise HTTPException(
                    status_code=HTTP_400_BAD_REQUEST, detail=err.args[0]
                )
    if key_lookups:
        # Duplicates are technically legal because *any* query can be given
        # with multiple parameters.
        unique_key_lookups = set(key_lookups)
        (key_lookup), *others = unique_key_lookups
        if others:
            # Two non-equal KeyLookup queries must return no results.
            tree = MapAdapter({})
        else:
            if hasattr(tree, "lookup_adapter"):
                try:
                    entry = await tree.lookup_adapter([key_lookup])
                    tree = MapAdapter({key_lookup: entry}, must_revalidate=False)
                except KeyError:
                    # If caught NoEntry or BrokenLink
                    tree = MapAdapter({})
            else:
                try:
                    tree = MapAdapter(
                        {key_lookup: tree[key_lookup]}, must_revalidate=False
                    )
                except KeyError:
                    tree = MapAdapter({})

    return tree


def apply_sort(tree, sort):
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
                status_code=HTTP_400_BAD_REQUEST,
                detail="This Tree does not support sorting.",
            )
        tree = tree.sort(sorting)

    return tree


async def construct_entries_response(
    query_registry,
    tree,
    route,
    path,
    offset,
    limit,
    fields,
    select_metadata,
    omit_links,
    include_data_sources,
    filters,
    sort,
    base_url,
    media_type,
    max_depth,
    exact_count_limit,
):
    "Construct a response for the `/search` endpoint"

    path_parts = [segment for segment in path.split("/") if segment]
    tree = await apply_search(tree, filters, query_registry)
    tree = apply_sort(tree, sort)

    count = await len_or_approx(
        tree, exact=(schemas.EntryFields.count in fields), threshold=exact_count_limit
    )
    links = pagination_links(base_url, route, path_parts, offset, limit, count)
    data = []

    if fields == [schemas.EntryFields.none]:
        # Pull a page of just the keys, which is cheaper.
        if hasattr(tree, "keys_range"):
            keys = await tree.keys_range(offset, limit)
        else:
            keys = tree.keys()[offset : offset + limit]  # noqa: E203
        items = [(key, None) for key in keys]
    elif fields == [schemas.EntryFields.count]:
        # Only count is requested, so we do not need to pull any items.
        items = []
    else:
        # Pull the entire page of full items into memory.
        if hasattr(tree, "items_range"):
            items = await tree.items_range(offset, limit)
        else:
            items = tree.items()[offset : offset + limit]  # noqa: E203

    # This value will not leak out. It just used to seed comparisons.
    metadata_stale_at = datetime.now(timezone.utc) + timedelta(days=1_000_000)
    must_revalidate = getattr(tree, "must_revalidate", True)
    for key, entry in items:
        resource = await construct_resource(
            base_url,
            path_parts + [key],
            entry,
            fields,
            select_metadata,
            omit_links,
            include_data_sources,
            media_type,
            max_depth=max_depth,
            exact_count_limit=exact_count_limit,
        )
        data.append(resource)
        # If any entry has entry.metadata_stale_at = None, then there will
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
    StructureFamily.array: {"*/*": "application/octet-stream", "image/*": "image/png"},
    StructureFamily.awkward: {"*/*": "application/zip"},
    StructureFamily.table: {"*/*": APACHE_ARROW_FILE_MIME_TYPE},
    StructureFamily.container: {"*/*": "application/x-hdf5"},
    StructureFamily.sparse: {"*/*": APACHE_ARROW_FILE_MIME_TYPE},
}


async def construct_revisions_response(
    entry,
    base_url,
    route,
    path,
    offset,
    limit,
    media_type,
):
    path_parts = [segment for segment in path.split("/") if segment]
    revisions = await entry.revisions(offset, limit)
    data = []
    for revision in revisions:
        item = {
            "revision_number": revision.revision_number,
            "attributes": {
                "metadata": revision.metadata,
                "specs": revision.specs,
                "time_updated": revision.time_updated,
            },
        }
        data.append(item)
    count = len(data)
    links = pagination_links(
        base_url, route, path_parts, offset, limit, count
    )  # maybe reuse or maybe make a new pagination_revision_links
    return schemas.Response(data=data, links=links, meta={"count": count})


async def construct_data_response(
    structure_family,
    serialization_registry,
    payload,
    metadata,
    request,
    format=None,
    specs=None,
    expires=None,
    filename=None,
    filter_for_access=None,
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
    base_media_type = None
    for media_type in media_types:
        if media_type == "*/*":
            media_type = DEFAULT_MEDIA_TYPES[structure_family]["*/*"]
        elif structure_family == StructureFamily.array and media_type == "image/*":
            media_type = DEFAULT_MEDIA_TYPES[structure_family]["image/*"]
        # Compare the request formats to the formats supported by each spec
        # name and, finally, by the structure family.
        for spec in [spec.name for spec in specs] + [structure_family]:
            media_types_for_spec = serialization_registry.media_types(spec)
            base_media_type = parse_mimetype(media_type)[0]
            if base_media_type in media_types_for_spec:
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
        etag = tokenize((payload, base_media_type))
    headers = {"ETag": etag}
    if expires is not None:
        headers["Expires"] = expires.strftime(HTTP_EXPIRES_HEADER_FORMAT)
    if request.headers.get("If-None-Match", "") == etag:
        # If the client already has this content, confirm that.
        return Response(status_code=HTTP_304_NOT_MODIFIED, headers=headers)
    if filename:
        headers["Content-Disposition"] = f'attachment; filename="{filename}"'
    serializer = serialization_registry.dispatch(spec, base_media_type)
    # This is the expensive step: actually serialize.
    try:
        if filter_for_access is not None:
            content = await ensure_awaitable(
                serializer, media_type, payload, metadata, filter_for_access
            )
        else:
            content = await ensure_awaitable(serializer, media_type, payload, metadata)
    except UnsupportedShape as err:
        raise UnsupportedMediaTypes(
            f"The shape of this data {err.args[0]} is incompatible with the requested format ({media_type}). "
            f"Slice it or choose a different format.",
        )
    except SerializationError as err:
        raise UnsupportedMediaTypes(
            f"This type is supported in general but there was an error packing this specific data: {err.args[0]}",
        )
    if inspect.isgenerator(content) or inspect.isasyncgen(content):
        response_class = StreamingResponse
    else:
        response_class = Response
    return response_class(
        content,
        media_type=media_type,
        headers=headers,
    )


async def construct_resource(
    base_url,
    path_parts,
    entry,
    fields,
    select_metadata,
    omit_links,
    include_data_sources,
    media_type,
    max_depth,
    exact_count_limit,
    depth=0,
):
    path_str = "/".join(path_parts)
    id_ = path_parts[-1] if path_parts else ""
    attributes = {"ancestors": path_parts[:-1]}
    if include_data_sources and hasattr(entry, "data_sources"):
        attributes["data_sources"] = entry.data_sources
    if schemas.EntryFields.metadata in fields:
        if select_metadata is not None:
            attributes["metadata"] = {
                "selected": jmespath.compile(select_metadata).search(entry.metadata())
            }
        else:
            attributes["metadata"] = entry.metadata()
    if schemas.EntryFields.access_blob in fields and hasattr(entry, "access_blob"):
        attributes["access_blob"] = entry.access_blob
    if schemas.EntryFields.specs in fields:
        attributes["specs"] = []
        for spec in getattr(entry, "specs", []):
            # back-compat for when a spec was just a string
            if isinstance(spec, str):
                spec = Spec(spec)
            attributes["specs"].append(spec)
    if (entry is not None) and entry.structure_family == StructureFamily.container:
        attributes["structure_family"] = entry.structure_family

        if (
            schemas.EntryFields.structure in fields
            or schemas.EntryFields.count in fields
        ):
            do_exact_count = fields == [schemas.EntryFields.count]
            count = await len_or_approx(
                entry, exact=do_exact_count, threshold=exact_count_limit
            )
            if (
                ((max_depth is None) or (depth < max_depth))
                and hasattr(entry, "inlined_contents_enabled")
                and entry.inlined_contents_enabled(depth)
                and depth <= DEPTH_LIMIT
            ):
                # This node wants us to inline its contents.
                # First check that it is not too large.
                if count > INLINED_CONTENTS_LIMIT:
                    # Too large: do not inline its contents.
                    contents = None
                else:
                    contents = {}
                    # The size may change as we are walking the entry.
                    # Get a new *true* count.
                    count = 0
                    for key in entry.keys():
                        count += 1
                        if count > INLINED_CONTENTS_LIMIT:
                            # The estimated count was inaccurate or else the entry has grown
                            # new children while we are walking it. Too large!
                            count = await len_or_approx(
                                entry, exact=do_exact_count, threshold=exact_count_limit
                            )
                            contents = None
                            break
                        try:
                            adapter = entry[key]
                        except BrokenLink:
                            # If there are any broken links (e.g. in HDF5), just keep the key
                            contents[key] = None
                            continue

                        contents[key] = await construct_resource(
                            base_url,
                            path_parts + [key],
                            adapter,
                            fields,
                            select_metadata,
                            omit_links,
                            include_data_sources,
                            media_type,
                            max_depth,
                            exact_count_limit,
                            depth=1 + depth,
                        )
            else:
                contents = None
            attributes["structure"] = schemas.NodeStructure(
                count=count,
                contents=contents,
            )

        if schemas.EntryFields.sorting in fields:
            if hasattr(entry, "sorting"):
                # HUGE HACK
                # In the Python API we encode sorting as (key, direction).
                # This order-based "record" notion does not play well with OpenAPI.
                # In the HTTP API, therefore, we use {"key": key, "direction": direction}.
                if entry.sorting and isinstance(entry.sorting[0], schemas.SortingItem):
                    attributes["sorting"] = entry.sorting
                else:
                    attributes["sorting"] = [
                        {"key": key, "direction": direction}
                        for key, direction in entry.sorting
                    ]
        d = {
            "id": id_,
            "attributes": schemas.NodeAttributes(**attributes),
        }

        if not omit_links:
            d["links"] = links_for_node(
                entry.structure_family,
                entry.structure(),
                base_url,
                path_str,
            )

        resource = schemas.Resource[
            schemas.NodeAttributes, schemas.ContainerLinks, schemas.ContainerMeta
        ](**d)
    else:
        links = {"self": f"{base_url}/metadata/{path_str}"}
        if entry is not None:
            # entry is None when we are pulling just *keys* from the
            # Tree and not values.
            ResourceLinksT = schemas.resource_links_type_by_structure_family[
                entry.structure_family
            ]
            links.update(
                links_for_node(
                    entry.structure_family,
                    entry.structure(),
                    base_url,
                    path_str,
                )
            )
            if schemas.EntryFields.structure_family in fields:
                attributes["structure_family"] = entry.structure_family
            if schemas.EntryFields.structure in fields:
                attributes["structure"] = asdict(entry.structure())

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
        # It is common in HTTP to fall back on a default representation if
        # none of the requested ones are available. We do not do this for
        # data payloads, but it makes some sense to do it for these metadata
        # messages.
        media_type = JSON_MIME_TYPE
    assert media_type in {JSON_MIME_TYPE, MSGPACK_MIME_TYPE}
    return media_type


def json_or_msgpack(
    request, content, expires=None, headers=None, status_code=HTTP_200_OK
):
    media_type = resolve_media_type(request)
    with record_timing(request.state.metrics, "tok"):
        etag = md5(str(content).encode()).hexdigest()
    headers = headers or {}
    headers["ETag"] = etag
    if expires is not None:
        headers["Expires"] = expires.strftime(HTTP_EXPIRES_HEADER_FORMAT)
    if request.headers.get("If-None-Match", "") == etag:
        # If the client already has this content, confirm that.
        return Response(status_code=HTTP_304_NOT_MODIFIED, headers=headers)
    if media_type == "application/x-msgpack":
        return MsgpackResponse(
            content,
            headers=headers,
            metrics=request.state.metrics,
            status_code=status_code,
        )
    return NumpySafeJSONResponse(
        content, headers=headers, metrics=request.state.metrics, status_code=status_code
    )


def get_websocket_envelope_formatter(
    envelope_format: schemas.EnvelopeFormat, entry, deserialization_registry
):
    if envelope_format == "msgpack":

        async def stream_msgpack(
            websocket: WebSocket,
            metadata: dict,
            payload_bytes: Optional[bytes],
        ):
            if payload_bytes is not None:
                metadata["payload"] = payload_bytes
            data = msgpack.packb(metadata)
            await websocket.send_bytes(data)

        return stream_msgpack

    elif envelope_format == "json":

        async def stream_json(
            websocket: WebSocket,
            metadata: dict,
            payload_bytes: Optional[bytes],
        ):
            if payload_bytes is not None:
                media_type = metadata.get("content-type", "application/octet-stream")
                if media_type == "application/json":
                    # nothing to do, the payload is already JSON
                    payload_decoded = payload_bytes
                else:
                    # Transcode to payload to JSON.
                    metadata["content-type"] = "application/json"
                    structure_family = (
                        StructureFamily.array
                    )  # TODO: generalize beyond array
                    structure = entry.structure()
                    deserializer = deserialization_registry.dispatch(
                        structure_family, media_type
                    )
                    payload_decoded = deserializer(
                        payload_bytes,
                        structure.data_type.to_numpy_dtype(),
                        metadata.get("shape"),
                    )
                metadata["payload"] = payload_decoded
            data = safe_json_dump(metadata)
            await websocket.send_text(data)

        return stream_json


class UnsupportedMediaTypes(Exception):
    pass


class NoEntry(KeyError):
    pass


class WrongTypeForRoute(Exception):
    pass


def asdict(dc):
    "Compat for converting dataclass or pydantic.BaseModel to dict."
    if dc is None:
        return None
    try:
        return dataclasses.asdict(dc)
    except TypeError:
        return dict(dc)
