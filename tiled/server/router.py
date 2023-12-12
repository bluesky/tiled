import dataclasses
import inspect
from datetime import datetime, timedelta
from functools import partial
from typing import Any, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Security
from jmespath.exceptions import JMESPathError
from pydantic import BaseSettings

from .. import __version__
from ..structures.core import StructureFamily
from ..utils import ensure_awaitable
from ..validation_registration import ValidationError
from . import schemas
from .authentication import Mode, get_authenticators, get_current_principal
from .core import (
    DEFAULT_PAGE_SIZE,
    DEPTH_LIMIT,
    MAX_PAGE_SIZE,
    NoEntry,
    UnsupportedMediaTypes,
    WrongTypeForRoute,
    apply_search,
    construct_data_response,
    construct_entries_response,
    construct_resource,
    construct_revisions_response,
    json_or_msgpack,
    resolve_media_type,
)
from .dependencies import (
    SecureEntry,
    block,
    expected_shape,
    get_deserialization_registry,
    get_query_registry,
    get_serialization_registry,
    get_validation_registry,
    slice_,
)
from .settings import get_settings
from .utils import filter_for_access, get_base_url, record_timing

router = APIRouter()


@router.get("/", response_model=schemas.About)
async def about(
    request: Request,
    settings: BaseSettings = Depends(get_settings),
    authenticators=Depends(get_authenticators),
    serialization_registry=Depends(get_serialization_registry),
    query_registry=Depends(get_query_registry),
    # This dependency is here because it runs the code that moves
    # API key from the query parameter to a cookie (if it is valid).
    principal=Security(get_current_principal, scopes=[]),
):
    # TODO The lazy import of entry modules and serializers means that the
    # lists of formats are not populated until they are first used. Not very
    # helpful for discovery! The registration can be made non-lazy, while the
    # imports of the underlying I/O libraries themselves (openpyxl, pillow,
    # etc.) can remain lazy.
    request.state.endpoint = "about"
    base_url = get_base_url(request)
    authentication = {
        "required": not settings.allow_anonymous_access,
    }
    provider_specs = []
    for provider, authenticator in authenticators.items():
        if authenticator.mode == Mode.password:
            spec = {
                "provider": provider,
                "mode": authenticator.mode.value,
                "links": {
                    "auth_endpoint": f"{base_url}/auth/provider/{provider}/token"
                },
                "confirmation_message": getattr(
                    authenticator, "confirmation_message", None
                ),
            }
        elif authenticator.mode == Mode.external:
            spec = {
                "provider": provider,
                "mode": authenticator.mode.value,
                "links": {
                    "auth_endpoint": f"{base_url}/auth/provider/{provider}/authorize"
                },
                "confirmation_message": getattr(
                    authenticator, "confirmation_message", None
                ),
            }
        else:
            # It should be impossible to reach here.
            assert False
        provider_specs.append(spec)
    if provider_specs:
        # If there are *any* authenticaiton providers, these
        # endpoints will be added.
        authentication["links"] = {
            "whoami": f"{base_url}/auth/whoami",
            "apikey": f"{base_url}/auth/apikey",
            "refresh_session": f"{base_url}/auth/session/refresh",
            "revoke_session": f"{base_url}/auth/session/revoke/{{session_id}}",
            "logout": f"{base_url}/auth/logout",
        }
    authentication["providers"] = provider_specs

    return json_or_msgpack(
        request,
        schemas.About(
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
            authentication=authentication,
            links={
                "self": base_url,
                "documentation": f"{base_url}/docs",
            },
            meta={"root_path": request.scope.get("root_path") or "" + "/api"},
        ).dict(),
        expires=datetime.utcnow() + timedelta(seconds=600),
    )


async def search(
    request: Request,
    path: str,
    fields: Optional[List[schemas.EntryFields]] = Query(list(schemas.EntryFields)),
    select_metadata: Optional[str] = Query(None),
    offset: Optional[int] = Query(0, alias="page[offset]", ge=0),
    limit: Optional[int] = Query(
        DEFAULT_PAGE_SIZE, alias="page[limit]", ge=0, le=MAX_PAGE_SIZE
    ),
    sort: Optional[str] = Query(None),
    max_depth: Optional[int] = Query(None, ge=0, le=DEPTH_LIMIT),
    omit_links: bool = Query(False),
    show_sources: bool = Query(False),
    entry: Any = SecureEntry(scopes=["read:metadata"]),
    query_registry=Depends(get_query_registry),
    principal: str = Depends(get_current_principal),
    **filters,
):
    request.state.endpoint = "search"
    if entry.structure_family != StructureFamily.container:
        raise WrongTypeForRoute("This is not a Node; it cannot be searched or listed.")
    entry = filter_for_access(
        entry, principal, ["read:metadata"], request.state.metrics
    )
    try:
        resource, metadata_stale_at, must_revalidate = await construct_entries_response(
            query_registry,
            entry,
            "/search",
            path,
            offset,
            limit,
            fields,
            select_metadata,
            omit_links,
            show_sources,
            filters,
            sort,
            get_base_url(request),
            resolve_media_type(request),
            max_depth=max_depth,
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
            resource.dict(),
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


async def distinct(
    request: Request,
    structure_families: bool = False,
    specs: bool = False,
    metadata: Optional[List[str]] = Query(default=[]),
    counts: bool = False,
    entry: Any = SecureEntry(scopes=["read:metadata"]),
    query_registry=Depends(get_query_registry),
    **filters,
):
    if hasattr(entry, "get_distinct"):
        filtered = await apply_search(entry, filters, query_registry)
        distinct = await ensure_awaitable(
            filtered.get_distinct, metadata, structure_families, specs, counts
        )

        return json_or_msgpack(
            request, schemas.GetDistinctResponse.parse_obj(distinct).dict()
        )
    else:
        raise HTTPException(
            status_code=405, detail="This node does not support distinct."
        )


def patch_route_signature(route, query_registry):
    """
    This is done dynamically at router startup.

    We check the registry of known search query types, which is user
    configurable, and use that to define the allowed HTTP query parameters for
    this route.

    Take a route that accept unspecified search queries as **filters.
    Return a wrapped version of the route that has the supported
    search queries explicitly spelled out in the function signature.

    This has no change in the actual behavior of the function,
    but it enables FastAPI to generate good OpenAPI documentation
    showing the supported search queries.

    """

    # Build a wrapper so that we can modify the signature
    # without mutating the wrapped original.

    async def route_with_sig(*args, **kwargs):
        return await route(*args, **kwargs)

    # Black magic here! FastAPI bases its validation and auto-generated swagger
    # documentation on the signature of the route function. We do not know what
    # that signature should be at compile-time. We only know it once we have a
    # chance to check the user-configurable registry of query types. Therefore,
    # we modify the signature here, at runtime, just before handing it to
    # FastAPI in the usual way.

    # When FastAPI calls the function with these added parameters, they will be
    # accepted via **filters.

    # Make a copy of the original parameters.
    signature = inspect.signature(route)
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
    route_with_sig.__signature__ = signature.replace(parameters=parameters)
    # End black magic

    return route_with_sig


@router.get(
    "/metadata/{path:path}",
    response_model=schemas.Response[
        schemas.Resource[schemas.NodeAttributes, dict, dict], dict, dict
    ],
)
async def metadata(
    request: Request,
    path: str,
    fields: Optional[List[schemas.EntryFields]] = Query(list(schemas.EntryFields)),
    select_metadata: Optional[str] = Query(None),
    max_depth: Optional[int] = Query(None, ge=0, le=DEPTH_LIMIT),
    omit_links: bool = Query(False),
    show_sources: bool = Query(False),
    entry: Any = SecureEntry(scopes=["read:metadata"]),
    root_path: bool = Query(False),
):
    "Fetch the metadata and structure information for one entry."

    request.state.endpoint = "metadata"
    base_url = get_base_url(request)
    path_parts = [segment for segment in path.split("/") if segment]
    try:
        resource = await construct_resource(
            base_url,
            path_parts,
            entry,
            fields,
            select_metadata,
            omit_links,
            show_sources,
            resolve_media_type(request),
            max_depth=max_depth,
        )
    except JMESPathError as err:
        raise HTTPException(
            status_code=400,
            detail=f"Malformed 'select_metadata' parameter raised JMESPathError: {err}",
        )
    meta = {"root_path": request.scope.get("root_path") or "/"} if root_path else {}
    return json_or_msgpack(
        request,
        schemas.Response(data=resource, meta=meta).dict(),
        expires=getattr(entry, "metadata_stale_at", None),
    )


@router.get(
    "/array/block/{path:path}", response_model=schemas.Response, name="array block"
)
async def array_block(
    request: Request,
    entry=SecureEntry(scopes=["read:data"]),
    block=Depends(block),
    slice=Depends(slice_),
    expected_shape=Depends(expected_shape),
    format: Optional[str] = None,
    filename: Optional[str] = None,
    serialization_registry=Depends(get_serialization_registry),
    settings: BaseSettings = Depends(get_settings),
):
    """
    Fetch a chunk of array-like data.
    """
    if entry.structure_family == "array":
        shape = entry.structure().shape
    elif entry.structure_family == "sparse":
        shape = entry.structure().shape
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Cannot read {entry.structure_family} structure with /array/block route.",
        )
    # Check that block dimensionality matches array dimensionality.
    ndim = len(shape)
    if len(block) != ndim:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Block parameter must have {ndim} comma-separated parameters, "
                f"corresponding to the dimensions of this {ndim}-dimensional array."
            ),
        )
    if block == ():
        # Handle special case of numpy scalar.
        if shape != ():
            raise HTTPException(
                status_code=400,
                detail=f"Requested scalar but shape is {entry.structure().shape}",
            )
        with record_timing(request.state.metrics, "read"):
            array = await ensure_awaitable(entry.read)
    else:
        try:
            with record_timing(request.state.metrics, "read"):
                array = await ensure_awaitable(entry.read_block, block, slice)
        except IndexError:
            raise HTTPException(status_code=400, detail="Block index out of range")
        if (expected_shape is not None) and (expected_shape != array.shape):
            raise HTTPException(
                status_code=400,
                detail=f"The expected_shape {expected_shape} does not match the actual shape {array.shape}",
            )
    if array.nbytes > settings.response_bytesize_limit:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Response would exceed {settings.response_bytesize_limit}. "
                "Use slicing ('?slice=...') to request smaller chunks."
            ),
        )
    try:
        with record_timing(request.state.metrics, "pack"):
            return await construct_data_response(
                entry.structure_family,
                serialization_registry,
                array,
                entry.metadata(),
                request,
                format,
                specs=getattr(entry, "specs", []),
                expires=getattr(entry, "content_stale_at", None),
                filename=filename,
            )
    except UnsupportedMediaTypes as err:
        # raise HTTPException(status_code=406, detail=", ".join(err.supported))
        raise HTTPException(status_code=406, detail=err.args[0])


@router.get(
    "/array/full/{path:path}", response_model=schemas.Response, name="full array"
)
async def array_full(
    request: Request,
    entry=SecureEntry(scopes=["read:data"]),
    slice=Depends(slice_),
    expected_shape=Depends(expected_shape),
    format: Optional[str] = None,
    filename: Optional[str] = None,
    serialization_registry=Depends(get_serialization_registry),
    settings: BaseSettings = Depends(get_settings),
):
    """
    Fetch a slice of array-like data.
    """
    structure_family = entry.structure_family
    if structure_family not in {"array", "sparse"}:
        raise HTTPException(
            status_code=404,
            detail=f"Cannot read {entry.structure_family} structure with /array/full route.",
        )
    # Deferred import because this is not a required dependency of the server
    # for some use cases.
    import numpy

    try:
        with record_timing(request.state.metrics, "read"):
            array = await ensure_awaitable(entry.read, slice)
        if structure_family == "array":
            array = numpy.asarray(array)  # Force dask or PIMS or ... to do I/O.
    except IndexError:
        raise HTTPException(status_code=400, detail="Block index out of range")
    if (expected_shape is not None) and (expected_shape != array.shape):
        raise HTTPException(
            status_code=400,
            detail=f"The expected_shape {expected_shape} does not match the actual shape {array.shape}",
        )
    if array.nbytes > settings.response_bytesize_limit:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Response would exceed {settings.response_bytesize_limit}. "
                "Use slicing ('?slice=...') to request smaller chunks."
            ),
        )
    try:
        with record_timing(request.state.metrics, "pack"):
            return await construct_data_response(
                structure_family,
                serialization_registry,
                array,
                entry.metadata(),
                request,
                format,
                specs=getattr(entry, "specs", []),
                expires=getattr(entry, "content_stale_at", None),
                filename=filename,
            )
    except UnsupportedMediaTypes as err:
        raise HTTPException(status_code=406, detail=err.args[0])


@router.get(
    "/table/partition/{path:path}",
    response_model=schemas.Response,
    name="table partition",
)
async def table_partition(
    request: Request,
    partition: int,
    entry=SecureEntry(scopes=["read:data"]),
    field: Optional[List[str]] = Query(None, min_length=1),
    format: Optional[str] = None,
    filename: Optional[str] = None,
    serialization_registry=Depends(get_serialization_registry),
    settings: BaseSettings = Depends(get_settings),
):
    """
    Fetch a partition (continuous block of rows) from a DataFrame.
    """
    if entry.structure_family != StructureFamily.table:
        raise HTTPException(
            status_code=404,
            detail=f"Cannot read {entry.structure_family} structure with /table/partition route.",
        )
    try:
        # The singular/plural mismatch here of "fields" and "field" is
        # due to the ?field=A&field=B&field=C... encodes in a URL.
        with record_timing(request.state.metrics, "read"):
            df = await ensure_awaitable(entry.read_partition, partition, field)
    except IndexError:
        raise HTTPException(status_code=400, detail="Partition out of range")
    except KeyError as err:
        (key,) = err.args
        raise HTTPException(status_code=400, detail=f"No such field {key}.")
    if df.memory_usage().sum() > settings.response_bytesize_limit:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Response would exceed {settings.response_bytesize_limit}. "
                "Select a subset of the columns ('?field=...') to "
                "request a smaller chunks."
            ),
        )
    try:
        with record_timing(request.state.metrics, "pack"):
            return await construct_data_response(
                StructureFamily.table,
                serialization_registry,
                df,
                entry.metadata(),
                request,
                format,
                specs=getattr(entry, "specs", []),
                expires=getattr(entry, "content_stale_at", None),
                filename=filename,
            )
    except UnsupportedMediaTypes as err:
        raise HTTPException(status_code=406, detail=err.args[0])


@router.get(
    "/table/full/{path:path}",
    response_model=schemas.Response,
    name="full 'table' data",
)
async def table_full(
    request: Request,
    entry=SecureEntry(scopes=["read:data"]),
    column: Optional[List[str]] = Query(None, min_length=1),
    format: Optional[str] = None,
    filename: Optional[str] = None,
    serialization_registry=Depends(get_serialization_registry),
    settings: BaseSettings = Depends(get_settings),
):
    """
    Fetch the data for the given table.
    """
    if entry.structure_family != StructureFamily.table:
        raise HTTPException(
            status_code=404,
            detail=f"Cannot read {entry.structure_family} structure with /table/full route.",
        )
    try:
        with record_timing(request.state.metrics, "read"):
            data = await ensure_awaitable(entry.read, column)
    except KeyError as err:
        (key,) = err.args
        raise HTTPException(status_code=400, detail=f"No such field {key}.")
    if data.memory_usage().sum() > settings.response_bytesize_limit:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Response would exceed {settings.response_bytesize_limit}. "
                "Select a subset of the columns to "
                "request a smaller chunks."
            ),
        )
    try:
        with record_timing(request.state.metrics, "pack"):
            return await construct_data_response(
                entry.structure_family,
                serialization_registry,
                data,
                entry.metadata(),
                request,
                format,
                specs=getattr(entry, "specs", []),
                expires=getattr(entry, "content_stale_at", None),
                filename=filename,
                filter_for_access=None,
            )
    except UnsupportedMediaTypes as err:
        raise HTTPException(status_code=406, detail=err.args[0])


@router.get(
    "/container/full/{path:path}",
    response_model=schemas.Response,
    name="full 'container' metadata and data",
)
async def container_full(
    request: Request,
    entry=SecureEntry(scopes=["read:data"]),
    principal: str = Depends(get_current_principal),
    field: Optional[List[str]] = Query(None, min_length=1),
    format: Optional[str] = None,
    filename: Optional[str] = None,
    serialization_registry=Depends(get_serialization_registry),
):
    """
    Fetch the data for the given container.
    """
    if entry.structure_family != StructureFamily.container:
        raise HTTPException(
            status_code=404,
            detail=f"Cannot read {entry.structure_family} structure with /container/full route.",
        )
    try:
        with record_timing(request.state.metrics, "read"):
            data = await ensure_awaitable(entry.read, field)
    except KeyError as err:
        (key,) = err.args
        raise HTTPException(status_code=400, detail=f"No such field {key}.")
    curried_filter = partial(
        filter_for_access,
        principal=principal,
        scopes=["read:data"],
        metrics=request.state.metrics,
    )
    # TODO Walk node to determine size before handing off to serializer.
    try:
        with record_timing(request.state.metrics, "pack"):
            return await construct_data_response(
                entry.structure_family,
                serialization_registry,
                data,
                entry.metadata(),
                request,
                format,
                specs=getattr(entry, "specs", []),
                expires=getattr(entry, "content_stale_at", None),
                filename=filename,
                filter_for_access=curried_filter,
            )
    except UnsupportedMediaTypes as err:
        raise HTTPException(status_code=406, detail=err.args[0])


@router.get(
    "/node/full/{path:path}",
    response_model=schemas.Response,
    name="full 'container' or 'table'",
    deprecated=True,
)
async def node_full(
    request: Request,
    entry=SecureEntry(scopes=["read:data"]),
    principal: str = Depends(get_current_principal),
    field: Optional[List[str]] = Query(None, min_length=1),
    format: Optional[str] = None,
    filename: Optional[str] = None,
    serialization_registry=Depends(get_serialization_registry),
    settings: BaseSettings = Depends(get_settings),
):
    """
    Fetch the data below the given node.
    """
    try:
        with record_timing(request.state.metrics, "read"):
            data = await ensure_awaitable(entry.read, field)
    except KeyError as err:
        (key,) = err.args
        raise HTTPException(status_code=400, detail=f"No such field {key}.")
    if (entry.structure_family == StructureFamily.table) and (
        data.memory_usage().sum() > settings.response_bytesize_limit
    ):
        raise HTTPException(
            status_code=400,
            detail=(
                f"Response would exceed {settings.response_bytesize_limit}. "
                "Select a subset of the columns ('?field=...') to "
                "request a smaller chunks."
            ),
        )
    if entry.structure_family == StructureFamily.container:
        curried_filter = partial(
            filter_for_access,
            principal=principal,
            scopes=["read:data"],
            metrics=request.state.metrics,
        )
    else:
        curried_filter = None
        # TODO Walk node to determine size before handing off to serializer.
    try:
        with record_timing(request.state.metrics, "pack"):
            return await construct_data_response(
                entry.structure_family,
                serialization_registry,
                data,
                entry.metadata(),
                request,
                format,
                specs=getattr(entry, "specs", []),
                expires=getattr(entry, "content_stale_at", None),
                filename=filename,
                filter_for_access=curried_filter,
            )
    except UnsupportedMediaTypes as err:
        raise HTTPException(status_code=406, detail=err.args[0])


@router.get(
    "/awkward/buffers/{path:path}",
    response_model=schemas.Response,
    name="AwkwardArray buffers",
)
async def get_awkward_buffers(
    request: Request,
    entry=SecureEntry(scopes=["read:data"]),
    form_key: Optional[List[str]] = Query(None, min_length=1),
    format: Optional[str] = None,
    filename: Optional[str] = None,
    serialization_registry=Depends(get_serialization_registry),
    settings: BaseSettings = Depends(get_settings),
):
    """
    Fetch a slice of AwkwardArray data.

    Note that there is a POST route on this same path with equivalent functionality.
    HTTP caches tends to engage with GET but not POST, so that GET route may be
    preferred for that reason. However, HTTP clients, servers, and proxies
    typically impose a length limit on URLs. (The HTTP spec does not specify
    one, but this is a pragmatic measure.) For requests with large numbers of
    form_key parameters, POST may be the only option.
    """
    return await _awkward_buffers(
        request=request,
        entry=entry,
        form_key=form_key,
        format=format,
        filename=filename,
        serialization_registry=serialization_registry,
        settings=settings,
    )


@router.post(
    "/awkward/buffers/{path:path}",
    response_model=schemas.Response,
    name="AwkwardArray buffers",
)
async def post_awkward_buffers(
    request: Request,
    body: List[str],
    entry=SecureEntry(scopes=["read:data"]),
    format: Optional[str] = None,
    filename: Optional[str] = None,
    serialization_registry=Depends(get_serialization_registry),
    settings: BaseSettings = Depends(get_settings),
):
    """
    Fetch a slice of AwkwardArray data.

    Note that there is a GET route on this same path with equivalent functionality.
    HTTP caches tends to engage with GET but not POST, so that GET route may be
    preferred for that reason. However, HTTP clients, servers, and proxies
    typically impose a length limit on URLs. (The HTTP spec does not specify
    one, but this is a pragmatic measure.) For requests with large numbers of
    form_key parameters, POST may be the only option.
    """
    return await _awkward_buffers(
        request=request,
        entry=entry,
        form_key=body,
        format=format,
        filename=filename,
        serialization_registry=serialization_registry,
        settings=settings,
    )


async def _awkward_buffers(
    request: Request,
    entry,
    form_key: Optional[List[str]],
    format: Optional[str],
    filename: Optional[str],
    serialization_registry,
    settings: BaseSettings,
):
    structure_family = entry.structure_family
    structure = entry.structure()
    if structure_family != StructureFamily.awkward:
        raise HTTPException(
            status_code=404,
            detail=f"Cannot read {entry.structure_family} structure with /awkward/buffers route.",
        )
    with record_timing(request.state.metrics, "read"):
        # The plural vs. singular mismatch is due to the way query parameters
        # are given as ?form_key=A&form_key=B&form_key=C.
        container = await ensure_awaitable(entry.read_buffers, form_key)
    if (
        sum(len(buffer) for buffer in container.values())
        > settings.response_bytesize_limit
    ):
        raise HTTPException(
            status_code=400,
            detail=(
                f"Response would exceed {settings.response_bytesize_limit}. "
                "Use slicing ('?slice=...') to request smaller chunks."
            ),
        )
    components = (structure.form, structure.length, container)
    try:
        with record_timing(request.state.metrics, "pack"):
            return await construct_data_response(
                structure_family,
                serialization_registry,
                components,
                entry.metadata(),
                request,
                format,
                specs=getattr(entry, "specs", []),
                expires=getattr(entry, "content_stale_at", None),
                filename=filename,
            )
    except UnsupportedMediaTypes as err:
        raise HTTPException(status_code=406, detail=err.args[0])


@router.get(
    "/awkward/full/{path:path}",
    response_model=schemas.Response,
    name="Full AwkwardArray",
)
async def awkward_full(
    request: Request,
    entry=SecureEntry(scopes=["read:data"]),
    # slice=Depends(slice_),
    format: Optional[str] = None,
    filename: Optional[str] = None,
    serialization_registry=Depends(get_serialization_registry),
    settings: BaseSettings = Depends(get_settings),
):
    """
    Fetch a slice of AwkwardArray data.
    """
    structure_family = entry.structure_family
    if structure_family != StructureFamily.awkward:
        raise HTTPException(
            status_code=404,
            detail=f"Cannot read {entry.structure_family} structure with /awkward/full route.",
        )
    # Deferred import because this is not a required dependency of the server
    # for some use cases.
    import awkward

    with record_timing(request.state.metrics, "read"):
        container = await ensure_awaitable(entry.read)
    structure = entry.structure()
    components = (structure.form, structure.length, container)
    array = awkward.from_buffers(*components)
    if array.nbytes > settings.response_bytesize_limit:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Response would exceed {settings.response_bytesize_limit}. "
                "Use slicing ('?slice=...') to request smaller chunks."
            ),
        )
    try:
        with record_timing(request.state.metrics, "pack"):
            return await construct_data_response(
                structure_family,
                serialization_registry,
                components,
                entry.metadata(),
                request,
                format,
                specs=getattr(entry, "specs", []),
                expires=getattr(entry, "content_stale_at", None),
                filename=filename,
            )
    except UnsupportedMediaTypes as err:
        raise HTTPException(status_code=406, detail=err.args[0])


@router.post("/metadata/{path:path}", response_model=schemas.PostMetadataResponse)
async def post_metadata(
    request: Request,
    path: str,
    body: schemas.PostMetadataRequest,
    validation_registry=Depends(get_validation_registry),
    settings: BaseSettings = Depends(get_settings),
    entry=SecureEntry(scopes=["write:metadata", "create"]),
):
    if not getattr(entry, "writable", False):
        raise HTTPException(
            status_code=405, detail=f"Data cannot be written at the path {path}"
        )

    metadata, structure_family, specs = (
        body.metadata,
        body.structure_family,
        body.specs,
    )
    if structure_family == StructureFamily.container:
        structure = None
    else:
        if len(body.data_sources) != 1:
            raise NotImplementedError
        structure = body.data_sources[0].structure

    metadata_modified = False

    # Specs should be ordered from most specific/constrained to least.
    # Validate them in reverse order, with the least constrained spec first,
    # because it may do normalization that helps pass the more constrained one.
    # Known Issue:
    # When there is more than one spec, it's possible for the validator for
    # Spec 2 to make a modification that breaks the validation for Spec 1.
    # For now we leave it to the server maintainer to ensure that validators
    # won't step on each other in this way, but this may need revisiting.
    for spec in reversed(specs):
        if spec.name not in validation_registry:
            if settings.reject_undeclared_specs:
                raise HTTPException(
                    status_code=400, detail=f"Unrecognized spec: {spec.name}"
                )
        else:
            validator = validation_registry(spec.name)
            try:
                result = validator(metadata, structure_family, structure, spec)
            except ValidationError as e:
                raise HTTPException(
                    status_code=400,
                    detail=f"failed validation for spec {spec.name}:\n{e}",
                )
            if result is not None:
                metadata_modified = True
                metadata = result

    key, node = await entry.create_node(
        metadata=body.metadata,
        structure_family=body.structure_family,
        key=body.id,
        specs=body.specs,
        data_sources=body.data_sources,
    )
    links = {}
    base_url = get_base_url(request)
    path_parts = [segment for segment in path.split("/") if segment] + [key]
    path_str = "/".join(path_parts)
    links["self"] = f"{base_url}/metadata/{path_str}"
    if body.structure_family in {StructureFamily.array, StructureFamily.sparse}:
        block_template = ",".join(
            f"{{{index}}}" for index in range(len(node.structure().shape))
        )
        links["block"] = f"{base_url}/array/block/{path_str}?block={block_template}"
        links["full"] = f"{base_url}/array/full/{path_str}"
    elif body.structure_family == StructureFamily.table:
        links[
            "partition"
        ] = f"{base_url}/table/partition/{path_str}?partition={{index}}"
        links["full"] = f"{base_url}/table/full/{path_str}"
    elif body.structure_family == StructureFamily.container:
        links["full"] = f"{base_url}/container/full/{path_str}"
        links["search"] = f"{base_url}/search/{path_str}"
    elif body.structure_family == StructureFamily.awkward:
        links["buffers"] = f"{base_url}/awkward/buffers/{path_str}"
        links["full"] = f"{base_url}/awkward/full/{path_str}"
    else:
        raise NotImplementedError(body.structure_family)
    response_data = {
        "id": key,
        "links": links,
        "data_sources": [ds.dict() for ds in node.data_sources],
    }
    if metadata_modified:
        response_data["metadata"] = metadata
    return json_or_msgpack(request, response_data)


@router.delete("/metadata/{path:path}")
async def delete(
    request: Request,
    entry=SecureEntry(scopes=["write:data", "write:metadata"]),
):
    if hasattr(entry, "delete"):
        await entry.delete()
    else:
        raise HTTPException(
            status_code=405, detail="This node does not support deletion."
        )
    return json_or_msgpack(request, None)


@router.put("/array/full/{path:path}")
async def put_array_full(
    request: Request,
    entry=SecureEntry(scopes=["write:data"]),
    deserialization_registry=Depends(get_deserialization_registry),
):
    body = await request.body()
    if not hasattr(entry, "write"):
        raise HTTPException(
            status_code=405, detail="This node cannot accept array data."
        )
    media_type = request.headers["content-type"]
    if entry.structure_family == "array":
        dtype = entry.structure().data_type.to_numpy_dtype()
        shape = entry.structure().shape
        deserializer = deserialization_registry.dispatch("array", media_type)
        data = await ensure_awaitable(deserializer, body, dtype, shape)
    elif entry.structure_family == "sparse":
        deserializer = deserialization_registry.dispatch("sparse", media_type)
        data = await ensure_awaitable(deserializer, body)
    else:
        raise NotImplementedError(entry.structure_family)
    await ensure_awaitable(entry.write, data)
    return json_or_msgpack(request, None)


@router.put("/array/block/{path:path}")
async def put_array_block(
    request: Request,
    entry=SecureEntry(scopes=["write:data"]),
    deserialization_registry=Depends(get_deserialization_registry),
    block=Depends(block),
):
    if not hasattr(entry, "write_block"):
        raise HTTPException(
            status_code=405, detail="This node cannot accept array data."
        )
    from tiled.adapters.array import slice_and_shape_from_block_and_chunks

    body = await request.body()
    media_type = request.headers["content-type"]
    if entry.structure_family == "array":
        dtype = entry.structure().data_type.to_numpy_dtype()
        _, shape = slice_and_shape_from_block_and_chunks(
            block, entry.structure().chunks
        )
        deserializer = deserialization_registry.dispatch("array", media_type)
        data = await ensure_awaitable(deserializer, body, dtype, shape)
    elif entry.structure_family == "sparse":
        deserializer = deserialization_registry.dispatch("sparse", media_type)
        data = await ensure_awaitable(deserializer, body)
    else:
        raise NotImplementedError(entry.structure_family)
    await ensure_awaitable(entry.write_block, data, block)
    return json_or_msgpack(request, None)


@router.put("/table/full/{path:path}")
@router.put("/node/full/{path:path}", deprecated=True)
async def put_node_full(
    request: Request,
    entry=SecureEntry(scopes=["write:data"]),
    deserialization_registry=Depends(get_deserialization_registry),
):
    if not hasattr(entry, "write"):
        raise HTTPException(
            status_code=405, detail="This node does not support writing."
        )
    body = await request.body()
    media_type = request.headers["content-type"]
    deserializer = deserialization_registry.dispatch(StructureFamily.table, media_type)
    data = await ensure_awaitable(deserializer, body)
    await ensure_awaitable(entry.write, data)
    return json_or_msgpack(request, None)


@router.put("/table/partition/{path:path}")
async def put_table_partition(
    partition: int,
    request: Request,
    entry=SecureEntry(scopes=["write:data"]),
    deserialization_registry=Depends(get_deserialization_registry),
):
    if not hasattr(entry, "write_partition"):
        raise HTTPException(
            status_code=405, detail="This node does not supporting writing a partition."
        )
    body = await request.body()
    media_type = request.headers["content-type"]
    deserializer = deserialization_registry.dispatch(StructureFamily.table, media_type)
    data = await ensure_awaitable(deserializer, body)
    await ensure_awaitable(entry.write_partition, data, partition)
    return json_or_msgpack(request, None)


@router.put("/awkward/full/{path:path}")
async def put_awkward_full(
    request: Request,
    entry=SecureEntry(scopes=["write:data"]),
    deserialization_registry=Depends(get_deserialization_registry),
):
    body = await request.body()
    if entry.structure_family != StructureFamily.awkward:
        raise HTTPException(
            status_code=404, detail="This route is not applicable to this node."
        )
    if not hasattr(entry, "write"):
        raise HTTPException(status_code=405, detail="This node cannot be written to.")
    media_type = request.headers["content-type"]
    deserializer = deserialization_registry.dispatch(
        StructureFamily.awkward, media_type
    )
    structure = entry.structure()
    data = await ensure_awaitable(deserializer, body, structure.form, structure.length)
    await ensure_awaitable(entry.write, data)
    return json_or_msgpack(request, None)


@router.put("/metadata/{path:path}", response_model=schemas.PutMetadataResponse)
async def put_metadata(
    request: Request,
    body: schemas.PutMetadataRequest,
    validation_registry=Depends(get_validation_registry),
    settings: BaseSettings = Depends(get_settings),
    entry=SecureEntry(scopes=["write:metadata"]),
):
    if not hasattr(entry, "update_metadata"):
        raise HTTPException(
            status_code=405, detail="This node does not support update of metadata."
        )

    metadata, structure_family, structure, specs = (
        body.metadata if body.metadata is not None else entry.metadata(),
        entry.structure_family,
        entry.structure(),
        body.specs if body.specs is not None else entry.specs,
    )

    metadata_modified = False

    # Specs should be ordered from most specific/constrained to least.
    # Validate them in reverse order, with the least constrained spec first,
    # because it may do normalization that helps pass the more constrained one.
    # Known Issue:
    # When there is more than one spec, it's possible for the validator for
    # Spec 2 to make a modification that breaks the validation for Spec 1.
    # For now we leave it to the server maintainer to ensure that validators
    # won't step on each other in this way, but this may need revisiting.
    for spec in reversed(specs):
        if spec.name not in validation_registry:
            if settings.reject_undeclared_specs:
                raise HTTPException(
                    status_code=400, detail=f"Unrecognized spec: {spec.name}"
                )
        else:
            validator = validation_registry(spec.name)
            try:
                result = validator(metadata, structure_family, structure, spec)
            except ValidationError as e:
                raise HTTPException(
                    status_code=400,
                    detail=f"failed validation for spec {spec.name}:\n{e}",
                )
            if result is not None:
                metadata_modified = True
                metadata = result

    await entry.update_metadata(metadata=metadata, specs=specs)

    response_data = {"id": entry.key}
    if metadata_modified:
        response_data["metadata"] = metadata
    return json_or_msgpack(request, response_data)


@router.get("/revisions/{path:path}")
async def get_revisions(
    request: Request,
    path: str,
    offset: Optional[int] = Query(0, alias="page[offset]", ge=0),
    limit: Optional[int] = Query(
        DEFAULT_PAGE_SIZE, alias="page[limit]", ge=0, le=MAX_PAGE_SIZE
    ),
    entry=SecureEntry(scopes=["read:metadata"]),
):
    if not hasattr(entry, "revisions"):
        raise HTTPException(
            status_code=405, detail="This node does not support revisions."
        )

    base_url = get_base_url(request)
    resource = await construct_revisions_response(
        entry,
        base_url,
        "/revisions",
        path,
        offset,
        limit,
        resolve_media_type(request),
    )
    return json_or_msgpack(request, resource.dict())


@router.delete("/revisions/{path:path}")
async def delete_revision(
    request: Request,
    number: int,
    entry=SecureEntry(scopes=["write:metadata"]),
):
    if not hasattr(entry, "revisions"):
        raise HTTPException(
            status_code=405,
            detail="This node does not support a del request for revisions.",
        )

    await entry.delete_revision(number)
    return json_or_msgpack(request, None)
