import dataclasses
import inspect
from datetime import datetime, timedelta
from functools import lru_cache
from typing import Any, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from jmespath.exceptions import JMESPathError
from pydantic import BaseSettings

from .. import __version__
from . import models
from .authentication import (
    API_KEY_COOKIE_NAME,
    check_single_user_api_key,
    get_authenticator,
)
from .core import (
    NoEntry,
    UnsupportedMediaTypes,
    WrongTypeForRoute,
    block,
    construct_data_response,
    construct_entries_response,
    construct_resource,
    entry,
    expected_shape,
    get_query_registry,
    get_serialization_registry,
    json_or_msgpack,
    record_timing,
    resolve_media_type,
    slice_,
)
from .settings import get_settings

DEFAULT_PAGE_SIZE = 100


router = APIRouter()


@router.get("/", response_model=models.About)
async def about(
    request: Request,
    has_single_user_api_key: str = Depends(check_single_user_api_key),
    settings: BaseSettings = Depends(get_settings),
    authenticator=Depends(get_authenticator),
    serialization_registry=Depends(get_serialization_registry),
    query_registry=Depends(get_query_registry),
):
    # TODO The lazy import of entry modules and serializers means that the
    # lists of formats are not populated until they are first used. Not very
    # helpful for discovery! The registration can be made non-lazy, while the
    # imports of the underlying I/O libraries themselves (openpyxl, pillow,
    # etc.) can remain lazy.
    request.state.endpoint = "about"
    if (authenticator is None) and has_single_user_api_key:
        if request.cookies.get(API_KEY_COOKIE_NAME) != settings.single_user_api_key:
            request.state.cookies_to_set.append(
                {"key": API_KEY_COOKIE_NAME, "value": settings.single_user_api_key}
            )
    if authenticator is None:
        auth_type = "api_key"
        auth_endpoint = None
    else:
        if authenticator.handles_credentials:
            auth_type = "password"
            auth_endpoint = None
        else:
            auth_type = "external"
            auth_endpoint = authenticator.authorization_endpoint

    return json_or_msgpack(
        request,
        models.About(
            library_version=__version__,
            api_version=0,
            formats={
                structure_family: list(
                    serialization_registry.media_types(structure_family)
                )
                for structure_family in serialization_registry.structure_families
            },
            aliases={
                structure_family: serialization_registry.aliases(structure_family)
                for structure_family in serialization_registry.structure_families
            },
            queries=list(query_registry.name_to_query_type),
            # documentation_url=".../docs",  # TODO How to get the base URL?
            meta={"root_path": request.scope.get("root_path") or "/"},
            authentication={
                "type": auth_type,
                "required": not settings.allow_anonymous_access,
                "endpoint": auth_endpoint,
                "confirmation_message": getattr(
                    authenticator, "confirmation_message", None
                ),
            },
        ),
        resolve_media_type(request),
        expires=datetime.utcnow() + timedelta(seconds=600),
    )


@lru_cache()
def prometheus_registry():
    """
    Configure prometheus_client.

    This is run the first time the /metrics endpoint is used.
    """
    # The multiprocess configuration makes it compatible with gunicorn.
    # https://github.com/prometheus/client_python/#multiprocess-mode-eg-gunicorn
    from prometheus_client import CollectorRegistry
    from prometheus_client.multiprocess import MultiProcessCollector

    registry = CollectorRegistry()
    MultiProcessCollector(registry)  # This has a side effect, apparently.
    return registry


@router.get("/metrics")
async def metrics(request: Request):
    """
    Prometheus metrics
    """
    from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

    request.state.endpoint = "metrics"
    data = generate_latest(prometheus_registry())
    return Response(data, headers={"Content-Type": CONTENT_TYPE_LATEST})


def declare_search_router(query_registry):
    """
    This is done dynamically at router startup.

    We check the registry of known search query types, which is user
    configurable, and use that to define the allowed HTTP query parameters for
    this route.
    """

    async def node_search(
        request: Request,
        path: str,
        fields: Optional[List[models.EntryFields]] = Query(list(models.EntryFields)),
        select_metadata: Optional[str] = Query(None),
        offset: Optional[int] = Query(0, alias="page[offset]"),
        limit: Optional[int] = Query(DEFAULT_PAGE_SIZE, alias="page[limit]"),
        sort: Optional[str] = Query(None),
        omit_links: bool = Query(False),
        entry: Any = Depends(entry),
        query_registry=Depends(get_query_registry),
        **filters,
    ):
        request.state.endpoint = "search"
        try:
            resource, metadata_stale_at, must_revalidate = construct_entries_response(
                query_registry,
                entry,
                "/node/search",
                path,
                offset,
                limit,
                fields,
                select_metadata,
                omit_links,
                filters,
                sort,
                _get_base_url(request),
                resolve_media_type(request),
            )
            # We only get one Expires header, so if different parts
            # of this response become stale at different times, we
            # cite the earliest one.
            entries_stale_at = getattr(entry, "entries_stale_at", None)
            headers = {}
            if (metadata_stale_at is None) or (entries_stale_at is None):
                expires = None
            else:
                expires = min(metadata_stale_at, entries_stale_at)
            if must_revalidate:
                headers["Cache-Control"] = "must-revalidate"
            return json_or_msgpack(
                request,
                resource,
                resolve_media_type(request),
                expires=expires,
                headers=headers,
            )
        except NoEntry:
            raise HTTPException(status_code=404, detail="No such entry.")
        except WrongTypeForRoute as err:
            raise HTTPException(status_code=404, detail=err.args[0])
        except JMESPathError as err:
            raise HTTPException(
                status_code=400,
                detail=f"Malformed 'select_metadata' parameter raised JMESPathError: {err}",
            )

    # Black magic here! FastAPI bases its validation and auto-generated swagger
    # documentation on the signature of the route function. We do not know what
    # that signature should be at compile-time. We only know it once we have a
    # chance to check the user-configurable registry of query types. Therefore,
    # we modify the signature here, at runtime, just before handing it to
    # FastAPI in the usual way.

    # When FastAPI calls the function with these added parameters, they will be
    # accepted via **filters.

    # Make a copy of the original parameters.
    signature = inspect.signature(node_search)
    parameters = list(signature.parameters.values())
    # Drop the **filters parameter from the signature.
    del parameters[-1]
    # Add a parameter for each field in each type of query.
    for name, query in query_registry.name_to_query_type.items():
        for field in dataclasses.fields(query):
            # The structured "alias" here is based on
            # https://mglaman.dev/blog/using-json-router-query-your-search-router-indexes
            if getattr(field.type, "__origin__", None) is list:
                field_type = str
            else:
                field_type = field.type
            injected_parameter = inspect.Parameter(
                name=f"filter___{name}___{field.name}",
                kind=inspect.Parameter.POSITIONAL_OR_KEYWORD,
                default=Query(None, alias=f"filter[{name}][condition][{field.name}]"),
                annotation=Optional[List[field_type]],
            )
            parameters.append(injected_parameter)
    node_search.__signature__ = signature.replace(parameters=parameters)
    # End black magic

    # Register the search route.
    router = APIRouter()
    router.get("/node/search", response_model=models.Response, include_in_schema=False)(
        node_search
    )
    router.get("/node/search/{path:path}", response_model=models.Response)(node_search)
    return router


@router.get("/node/metadata/{path:path}", response_model=models.Response)
async def node_metadata(
    request: Request,
    path: str,
    fields: Optional[List[models.EntryFields]] = Query(list(models.EntryFields)),
    select_metadata: Optional[str] = Query(None),
    omit_links: bool = Query(False),
    entry: Any = Depends(entry),
    root_path: str = Query(None),
):
    "Fetch the metadata and structure information for one entry."

    request.state.endpoint = "metadata"
    base_url = _get_base_url(request)
    path_parts = [segment for segment in path.split("/") if segment]
    try:
        resource = construct_resource(
            base_url,
            path_parts,
            entry,
            fields,
            select_metadata,
            omit_links,
            resolve_media_type(request),
        )
    except JMESPathError as err:
        raise HTTPException(
            status_code=400,
            detail=f"Malformed 'select_metadata' parameter raised JMESPathError: {err}",
        )
    meta = (
        {"root_path": request.scope.get("root_path") or "/"}
        if (root_path is not None)
        else {}
    )
    return json_or_msgpack(
        request,
        models.Response(data=resource, meta=meta),
        resolve_media_type(request),
        expires=getattr(entry, "metadata_stale_at", None),
    )


@router.get(
    "/array/block/{path:path}", response_model=models.Response, name="array block"
)
def array_block(
    request: Request,
    entry=Depends(entry),
    block=Depends(block),
    slice=Depends(slice_),
    expected_shape=Depends(expected_shape),
    format: Optional[str] = None,
    serialization_registry=Depends(get_serialization_registry),
):
    """
    Fetch a chunk of array-like data.
    """
    if block == ():
        # Handle special case of numpy scalar.
        if entry.macrostructure().shape != ():
            raise HTTPException(
                status_code=400,
                detail=f"Requested scalar but shape is {entry.macrostructure().shape}",
            )
        with record_timing(request.state.metrics, "read"):
            array = entry.read()
    else:
        try:
            with record_timing(request.state.metrics, "read"):
                array = entry.read_block(block, slice=slice)
        except IndexError:
            raise HTTPException(status_code=400, detail="Block index out of range")
        if (expected_shape is not None) and (expected_shape != array.shape):
            raise HTTPException(
                status_code=400,
                detail=f"The expected_shape {expected_shape} does not match the actual shape {array.shape}",
            )
    try:
        with record_timing(request.state.metrics, "pack"):
            return construct_data_response(
                "array",
                serialization_registry,
                array,
                entry.metadata,
                request,
                format,
                expires=getattr(entry, "content_stale_at", None),
            )
    except UnsupportedMediaTypes as err:
        # raise HTTPException(status_code=406, detail=", ".join(err.supported))
        raise HTTPException(status_code=406, detail=err.args[0])


@router.get(
    "/array/full/{path:path}", response_model=models.Response, name="full array"
)
def array_full(
    request: Request,
    entry=Depends(entry),
    slice=Depends(slice_),
    expected_shape=Depends(expected_shape),
    format: Optional[str] = None,
    serialization_registry=Depends(get_serialization_registry),
):
    """
    Fetch a slice of array-like data.
    """
    # Deferred import because this is not a required dependency of the server
    # for some use cases.
    import numpy

    try:
        with record_timing(request.state.metrics, "read"):
            array = entry.read(slice)
        array = numpy.asarray(array)  # Force dask or PIMS or ... to do I/O.
    except IndexError:
        raise HTTPException(status_code=400, detail="Block index out of range")
    if (expected_shape is not None) and (expected_shape != array.shape):
        raise HTTPException(
            status_code=400,
            detail=f"The expected_shape {expected_shape} does not match the actual shape {array.shape}",
        )
    try:
        with record_timing(request.state.metrics, "pack"):
            return construct_data_response(
                "array",
                serialization_registry,
                array,
                entry.metadata,
                request,
                format,
                expires=getattr(entry, "content_stale_at", None),
            )
    except UnsupportedMediaTypes as err:
        raise HTTPException(status_code=406, detail=err.args[0])


@router.get(
    "/dataframe/partition/{path:path}",
    response_model=models.Response,
    name="dataframe partition",
)
def dataframe_partition(
    request: Request,
    partition: int,
    entry=Depends(entry),
    field: Optional[List[str]] = Query(None, min_length=1),
    format: Optional[str] = None,
    serialization_registry=Depends(get_serialization_registry),
):
    """
    Fetch a partition (continuous block of rows) from a DataFrame.
    """
    if entry.structure_family != "dataframe":
        raise HTTPException(
            status_code=404,
            detail=f"Cannot read {entry.structure_family} structure with /dataframe/partition route.",
        )
    try:
        # The singular/plural mismatch here of "fields" and "field" is
        # due to the ?field=A&field=B&field=C... encodes in a URL.
        with record_timing(request.state.metrics, "read"):
            df = entry.read_partition(partition, fields=field)
    except IndexError:
        raise HTTPException(status_code=400, detail="Partition out of range")
    except KeyError as err:
        (key,) = err.args
        raise HTTPException(status_code=400, detail=f"No such field {key}.")
    try:
        with record_timing(request.state.metrics, "pack"):
            return construct_data_response(
                "dataframe",
                serialization_registry,
                df,
                entry.metadata,
                request,
                format,
                expires=getattr(entry, "content_stale_at", None),
            )
    except UnsupportedMediaTypes as err:
        raise HTTPException(status_code=406, detail=err.args[0])


@router.get(
    "/node/full/{path:path}",
    response_model=models.Response,
    name="full xarray.Dataset",
)
def node_full(
    request: Request,
    entry=Depends(entry),
    field: Optional[List[str]] = Query(None, min_length=1),
    format: Optional[str] = None,
    serialization_registry=Depends(get_serialization_registry),
):
    """
    Fetch the data below the given node.
    """
    try:
        # The singular/plural mismatch here of "fields" and "field" is
        # due to the ?field=A&field=B&field=C... encodes in a URL.
        with record_timing(request.state.metrics, "read"):
            data = entry.read(fields=field)
    except KeyError as err:
        (key,) = err.args
        raise HTTPException(status_code=400, detail=f"No such field {key}.")
    try:
        with record_timing(request.state.metrics, "pack"):
            return construct_data_response(
                entry.structure_family,
                serialization_registry,
                data,
                entry.metadata,
                request,
                format,
                expires=getattr(entry, "content_stale_at", None),
            )
    except UnsupportedMediaTypes as err:
        raise HTTPException(status_code=406, detail=err.args[0])


def _get_base_url(request):
    # We want to get the scheme, host, and root_path (if any)
    # *as it appears to the client* for use in assembling links to
    # include in our responses.
    #
    # We need to consider:
    #
    # * FastAPI may be behind a load balancer, such that for a client request
    #   like "https://example.com/..." the Host header is set to something
    #   like "localhost:8000" and the request.url.scheme is "http".
    #   We consult X-Forwarded-* headers to get the original Host and scheme.
    #   Note that, although these are a de facto standard, they may not be
    #   set by default. With nginx, for example, they need to be configured.
    #
    # * The client may be connecting through SSH port-forwarding. (This
    #   is a niche use case but one that we nonetheless care about.)
    #   The Host or X-Forwarded-Host header may include a non-default port.
    #   The HTTP spec specifies that the Host header may include a port
    #   to specify a non-default port.
    #   https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.23
    host = request.headers.get("x-forwarded-host", request.headers["host"])
    scheme = request.headers.get("x-forwarded-proto", request.url.scheme)
    root_path = request.scope.get("root_path") or "/"
    if not root_path.endswith("/"):
        root_path = f"{root_path}/"
    return f"{scheme}://{host}{root_path}"
