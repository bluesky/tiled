import dataclasses
import inspect
from datetime import datetime, timedelta
from typing import Any, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Security
from jmespath.exceptions import JMESPathError
from pydantic import BaseSettings

from .. import __version__
from . import schemas
from .authentication import Mode, get_authenticators, get_current_principal
from .core import (
    NoEntry,
    UnsupportedMediaTypes,
    WrongTypeForRoute,
    construct_data_response,
    construct_entries_response,
    construct_resource,
    json_or_msgpack,
    resolve_media_type,
)
from .dependencies import (
    block,
    entry,
    expected_shape,
    get_query_registry,
    get_serialization_registry,
    slice_,
)
from .settings import get_settings
from .utils import get_base_url, record_timing

DEFAULT_PAGE_SIZE = 100


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
            endpoint = authenticator.authorization_endpoint
            if endpoint.startswith("/"):
                # This is relative.
                endpoint = f"{base_url}/auth/provider/{provider}{endpoint}"
            spec = {
                "provider": provider,
                "mode": authenticator.mode.value,
                "links": {"auth_endpoint": endpoint},
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
                "documentation": f"{base_url}docs",
            },
            meta={"root_path": request.scope.get("root_path") or "" + "/api"},
        ),
        expires=datetime.utcnow() + timedelta(seconds=600),
    )


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
        fields: Optional[List[schemas.EntryFields]] = Query(list(schemas.EntryFields)),
        select_metadata: Optional[str] = Query(None),
        offset: Optional[int] = Query(0, alias="page[offset]"),
        limit: Optional[int] = Query(DEFAULT_PAGE_SIZE, alias="page[limit]"),
        sort: Optional[str] = Query(None),
        omit_links: bool = Query(False),
        entry: Any = Security(entry, scopes=["read:metadata"]),
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
                get_base_url(request),
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
    router.get(
        "/node/search",
        response_model=schemas.Response[
            List[schemas.Resource[schemas.NodeAttributes, dict, dict]],
            schemas.PaginationLinks,
            dict,
        ],
        include_in_schema=False,
    )(node_search)
    router.get(
        "/node/search/{path:path}",
        response_model=schemas.Response[
            List[schemas.Resource[schemas.NodeAttributes, dict, dict]],
            schemas.PaginationLinks,
            dict,
        ],
    )(node_search)
    return router


@router.get(
    "/node/metadata/{path:path}",
    response_model=schemas.Response[
        schemas.Resource[schemas.NodeAttributes, dict, dict], dict, dict
    ],
)
async def node_metadata(
    request: Request,
    path: str,
    fields: Optional[List[schemas.EntryFields]] = Query(list(schemas.EntryFields)),
    select_metadata: Optional[str] = Query(None),
    omit_links: bool = Query(False),
    entry: Any = Security(entry, scopes=["read:metadata"]),
    root_path: bool = Query(False),
):
    "Fetch the metadata and structure information for one entry."

    request.state.endpoint = "metadata"
    base_url = get_base_url(request)
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
    meta = {"root_path": request.scope.get("root_path") or "/"} if root_path else {}
    return json_or_msgpack(
        request,
        schemas.Response(data=resource, meta=meta),
        expires=getattr(entry, "metadata_stale_at", None),
    )


@router.get(
    "/array/block/{path:path}", response_model=schemas.Response, name="array block"
)
def array_block(
    request: Request,
    entry=Security(entry, scopes=["read:data"]),
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
    if entry.structure_family not in {"array", "xarray_data_array"}:
        raise HTTPException(
            status_code=404,
            detail=f"Cannot read {entry.structure_family} structure with /array/block route.",
        )
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
    if array.nbytes > settings.response_bytesize_limit:
        raise HTTPException(
            status_code=400,
            detail=(
                "Response would exceed {settings.response_bytesize_limit}. "
                "Use slicing ('?slice=...') to request smaller chunks."
            ),
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
                filename=filename,
            )
    except UnsupportedMediaTypes as err:
        # raise HTTPException(status_code=406, detail=", ".join(err.supported))
        raise HTTPException(status_code=406, detail=err.args[0])


@router.get(
    "/array/full/{path:path}", response_model=schemas.Response, name="full array"
)
def array_full(
    request: Request,
    entry=Security(entry, scopes=["read:data"]),
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
    if entry.structure_family not in {"array", "xarray_data_array"}:
        raise HTTPException(
            status_code=404,
            detail=f"Cannot read {entry.structure_family} structure with /array/full route.",
        )
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
    if array.nbytes > settings.response_bytesize_limit:
        raise HTTPException(
            status_code=400,
            detail=(
                "Response would exceed {settings.response_bytesize_limit}. "
                "Use slicing ('?slice=...') to request smaller chunks."
            ),
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
                filename=filename,
            )
    except UnsupportedMediaTypes as err:
        raise HTTPException(status_code=406, detail=err.args[0])


@router.get(
    "/dataframe/partition/{path:path}",
    response_model=schemas.Response,
    name="dataframe partition",
)
def dataframe_partition(
    request: Request,
    partition: int,
    entry=Security(entry, scopes=["read:data"]),
    field: Optional[List[str]] = Query(None, min_length=1),
    format: Optional[str] = None,
    filename: Optional[str] = None,
    serialization_registry=Depends(get_serialization_registry),
    settings: BaseSettings = Depends(get_settings),
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
    if df.memory_usage().sum() > settings.response_bytesize_limit:
        raise HTTPException(
            status_code=400,
            detail=(
                "Response would exceed {settings.response_bytesize_limit}. "
                "Select a subset of the columns ('?field=...') to "
                "request a smaller chunks."
            ),
        )
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
                filename=filename,
            )
    except UnsupportedMediaTypes as err:
        raise HTTPException(status_code=406, detail=err.args[0])


@router.get(
    "/node/full/{path:path}",
    response_model=schemas.Response,
    name="full generic 'node', dataframe, or xarray Dataset",
)
def node_full(
    request: Request,
    entry=Security(entry, scopes=["read:data"]),
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
        # The singular/plural mismatch here of "fields" and "field" is
        # due to the ?field=A&field=B&field=C... encodes in a URL.
        with record_timing(request.state.metrics, "read"):
            data = entry.read(fields=field)
    except KeyError as err:
        (key,) = err.args
        raise HTTPException(status_code=400, detail=f"No such field {key}.")
    if (entry.structure_family == "dataframe") and (
        data.memory_usage().sum() > settings.response_bytesize_limit
    ):
        raise HTTPException(
            status_code=400,
            detail=(
                "Response would exceed {settings.response_bytesize_limit}. "
                "Select a subset of the columns ('?field=...') to "
                "request a smaller chunks."
            ),
        )
    # With a generic 'node' we cannot know at this point how large it
    # will be. We rely on the serializers to give up if they discover too
    # much data. Once we support asynchronous workers, we can default to or
    # require async packing for generic nodes.
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
                filename=filename,
            )
    except UnsupportedMediaTypes as err:
        raise HTTPException(status_code=406, detail=err.args[0])
