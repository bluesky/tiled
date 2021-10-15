import dataclasses
from hashlib import md5
import inspect
from typing import Any, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest, REGISTRY
from pydantic import BaseSettings

from .authentication import (
    API_KEY_COOKIE_NAME,
    get_authenticator,
    check_single_user_api_key,
)
from .settings import get_settings

from .core import (
    APACHE_ARROW_FILE_MIME_TYPE,
    block,
    construct_data_response,
    construct_entries_response,
    construct_resource,
    get_query_registry,
    get_serialization_registry,
    reader,
    entry,
    expected_shape,
    json_or_msgpack,
    NoEntry,
    PatchedResponse,
    record_timing,
    slice_,
    WrongTypeForRoute,
    UnsupportedMediaTypes,
)
from . import models
from .. import __version__


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
    # TODO The lazy import of reader modules and serializers means that the
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
    )


@router.get("/metrics")
async def metrics(request: Request):
    """
    Prometheus metrics
    """
    request.state.endpoint = "metrics"
    return Response(
        generate_latest(REGISTRY), headers={"Content-Type": CONTENT_TYPE_LATEST}
    )


def declare_search_router(query_registry):
    """
    This is done dynamically at router startup.

    We check the registry of known search query types, which is user
    configurable, and use that to define the allowed HTTP query parameters for
    this route.
    """

    async def search(
        request: Request,
        path: str,
        fields: Optional[List[models.EntryFields]] = Query(list(models.EntryFields)),
        offset: Optional[int] = Query(0, alias="page[offset]"),
        limit: Optional[int] = Query(DEFAULT_PAGE_SIZE, alias="page[limit]"),
        sort: Optional[str] = Query(None),
        entry: Any = Depends(entry),
        query_registry=Depends(get_query_registry),
        **filters,
    ):
        request.state.endpoint = "search"
        try:
            return json_or_msgpack(
                request,
                construct_entries_response(
                    query_registry,
                    entry,
                    "/search",
                    path,
                    offset,
                    limit,
                    fields,
                    filters,
                    sort,
                    _get_base_url(request),
                ),
            )
        except NoEntry:
            raise HTTPException(status_code=404, detail="No such entry.")
        except WrongTypeForRoute as err:
            raise HTTPException(status_code=404, detail=err.args[0])

    # Black magic here! FastAPI bases its validation and auto-generated swagger
    # documentation on the signature of the route function. We do not know what
    # that signature should be at compile-time. We only know it once we have a
    # chance to check the user-configurable registry of query types. Therefore,
    # we modify the signature here, at runtime, just before handing it to
    # FastAPI in the usual way.

    # When FastAPI calls the function with these added parameters, they will be
    # accepted via **filters.

    # Make a copy of the original parameters.
    signature = inspect.signature(search)
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
    search.__signature__ = signature.replace(parameters=parameters)
    # End black magic

    # Register the search route.
    router = APIRouter()
    router.get("/search", response_model=models.Response, include_in_schema=False)(
        search
    )
    router.get("/search/{path:path}", response_model=models.Response)(search)
    return router


@router.get("/metadata/{path:path}", response_model=models.Response)
async def metadata(
    request: Request,
    path: str,
    fields: Optional[List[models.EntryFields]] = Query(list(models.EntryFields)),
    entry: Any = Depends(entry),
    root_path: str = Query(None),
    settings: BaseSettings = Depends(get_settings),
):
    "Fetch the metadata for one Tree or Reader."

    request.state.endpoint = "metadata"
    base_url = _get_base_url(request)
    path_parts = [segment for segment in path.split("/") if segment]
    resource = construct_resource(base_url, path_parts, entry, fields)
    meta = (
        {"root_path": request.scope.get("root_path") or "/"}
        if (root_path is not None)
        else {}
    )
    return json_or_msgpack(request, models.Response(data=resource, meta=meta))


@router.get("/entries/{path:path}", response_model=models.Response)
async def entries(
    request: Request,
    path: Optional[str],
    offset: Optional[int] = Query(0, alias="page[offset]"),
    limit: Optional[int] = Query(DEFAULT_PAGE_SIZE, alias="page[limit]"),
    sort: Optional[str] = Query(None),
    fields: Optional[List[models.EntryFields]] = Query(list(models.EntryFields)),
    entry: Any = Depends(entry),
    query_registry=Depends(get_query_registry),
):
    "List the entries in a Tree, which may be sub-Trees or Readers."

    request.state.endpoint = "entries"
    try:
        return json_or_msgpack(
            request,
            construct_entries_response(
                query_registry,
                entry,
                "/entries",
                path,
                offset,
                limit,
                fields,
                {},
                sort,
                _get_base_url(request),
            ),
        )
    except NoEntry:
        raise HTTPException(status_code=404, detail="No such entry.")
    except WrongTypeForRoute as err:
        raise HTTPException(status_code=404, detail=err.args[0])


@router.get(
    "/array/block/{path:path}", response_model=models.Response, name="array block"
)
def array_block(
    request: Request,
    reader=Depends(reader),
    block=Depends(block),
    slice=Depends(slice_),
    expected_shape=Depends(expected_shape),
    format: Optional[str] = None,
    serialization_registry=Depends(get_serialization_registry),
):
    """
    Fetch a chunk of array-like data.
    """
    if reader.structure_family != "array":
        raise HTTPException(
            status_code=404,
            detail=f"Cannot read {reader.structure_family} structure with /array/block route.",
        )
    if block == ():
        # Handle special case of numpy scalar.
        if reader.macrostructure().shape != ():
            raise HTTPException(
                status_code=400,
                detail=f"Requested scalar but shape is {reader.macrostructure().shape}",
            )
        with record_timing(request.state.metrics, "read"):
            array = reader.read()
    else:
        try:
            with record_timing(request.state.metrics, "read"):
                array = reader.read_block(block, slice=slice)
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
                reader.metadata,
                request,
                format,
            )
    except UnsupportedMediaTypes as err:
        raise HTTPException(status_code=406, detail=", ".join(err.supported))


@router.get(
    "/array/full/{path:path}", response_model=models.Response, name="full array"
)
def array_full(
    request: Request,
    reader=Depends(reader),
    slice=Depends(slice_),
    expected_shape=Depends(expected_shape),
    format: Optional[str] = None,
    serialization_registry=Depends(get_serialization_registry),
):
    """
    Fetch a slice of array-like data.
    """
    if reader.structure_family != "array":
        raise HTTPException(
            status_code=404,
            detail=f"Cannot read {reader.structure_family} structure with /array/full route.",
        )
    # Deferred import because this is not a required dependency of the server
    # for some use cases.
    import numpy

    try:
        with record_timing(request.state.metrics, "read"):
            array = reader.read(slice)
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
                reader.metadata,
                request,
                format,
            )
    except UnsupportedMediaTypes as err:
        raise HTTPException(status_code=406, detail=", ".join(err.supported))


@router.get(
    "/structured_array_generic/block/{path:path}",
    response_model=models.Response,
    name="structured array (generic) block",
)
def structured_array_generic_block(
    request: Request,
    reader=Depends(reader),
    block=Depends(block),
    slice=Depends(slice_),
    expected_shape=Depends(expected_shape),
    format: Optional[str] = None,
    serialization_registry=Depends(get_serialization_registry),
):
    """
    Fetch a chunk of array-like data.
    """
    if reader.structure_family != "structured_array_generic":
        raise HTTPException(
            status_code=404,
            detail=f"Cannot read {reader.structure_family} structure with /structured_array_generic/block route.",
        )
    if block == ():
        # Handle special case of numpy scalar.
        if reader.macrostructure().shape != ():
            raise HTTPException(
                status_code=400,
                detail=f"Requested scalar but shape is {reader.macrostructure().shape}",
            )
        array = reader.read()
    else:
        try:
            array = reader.read_block(block, slice=slice)
        except IndexError:
            raise HTTPException(status_code=400, detail="Block index out of range")
        if (expected_shape is not None) and (expected_shape != array.shape):
            raise HTTPException(
                status_code=400,
                detail=f"The expected_shape {expected_shape} does not match the actual shape {array.shape}",
            )
    try:
        return construct_data_response(
            "structured_array_generic",
            serialization_registry,
            array,
            reader.metadata,
            request,
            format,
        )
    except UnsupportedMediaTypes as err:
        raise HTTPException(status_code=406, detail=", ".join(err.supported))


@router.get(
    "/structured_array_tabular/block/{path:path}",
    response_model=models.Response,
    name="structured array (tabular) block",
)
def structured_array_tabular_block(
    request: Request,
    reader=Depends(reader),
    block=Depends(block),
    slice=Depends(slice_),
    expected_shape=Depends(expected_shape),
    format: Optional[str] = None,
    serialization_registry=Depends(get_serialization_registry),
):
    """
    Fetch a chunk of array-like data.
    """
    if reader.structure_family != "structured_array_tabular":
        raise HTTPException(
            status_code=404,
            detail=f"Cannot read {reader.structure_family} structure with /structured_array_tabular/block route.",
        )
    if block == ():
        # Handle special case of numpy scalar.
        if reader.macrostructure().shape != ():
            raise HTTPException(
                status_code=400,
                detail=f"Requested scalar but shape is {reader.macrostructure().shape}",
            )
        array = reader.read()
    else:
        try:
            array = reader.read_block(block, slice=slice)
        except IndexError:
            raise HTTPException(status_code=400, detail="Block index out of range")
        if (expected_shape is not None) and (expected_shape != array.shape):
            raise HTTPException(
                status_code=400,
                detail=f"The expected_shape {expected_shape} does not match the actual shape {array.shape}",
            )
    try:
        return construct_data_response(
            "structured_array_tabular",
            serialization_registry,
            array,
            reader.metadata,
            request,
            format,
        )
    except UnsupportedMediaTypes as err:
        raise HTTPException(status_code=406, detail=", ".join(err.supported))


@router.get(
    "/structured_array_tabular/full/{path:path}",
    response_model=models.Response,
    name="structure array (tabular) full array",
)
def structured_array_tabular_full(
    request: Request,
    reader=Depends(reader),
    slice=Depends(slice_),
    expected_shape=Depends(expected_shape),
    format: Optional[str] = None,
    serialization_registry=Depends(get_serialization_registry),
):
    """
    Fetch a slice of array-like data.
    """
    if reader.structure_family != "structured_array_tabular":
        raise HTTPException(
            status_code=404,
            detail=f"Cannot read {reader.structure_family} structure with /structured_array_tabular/full route.",
        )
    # Deferred import because this is not a required dependency of the server
    # for some use cases.
    import numpy

    try:
        array = reader.read()
        if slice:
            array = array[slice]
        array = numpy.asarray(array)  # Force dask or PIMS or ... to do I/O.
    except IndexError:
        raise HTTPException(status_code=400, detail="Block index out of range")
    if (expected_shape is not None) and (expected_shape != array.shape):
        raise HTTPException(
            status_code=400,
            detail=f"The expected_shape {expected_shape} does not match the actual shape {array.shape}",
        )
    try:
        return construct_data_response(
            "structured_array_tabular",
            serialization_registry,
            array,
            reader.metadata,
            request,
            format,
        )
    except UnsupportedMediaTypes as err:
        raise HTTPException(status_code=406, detail=", ".join(err.supported))


@router.get(
    "/structured_array_generic/full/{path:path}",
    response_model=models.Response,
    name="structured array (generic) full array",
)
def structured_array_generic_full(
    request: Request,
    reader=Depends(reader),
    slice=Depends(slice_),
    expected_shape=Depends(expected_shape),
    format: Optional[str] = None,
    serialization_registry=Depends(get_serialization_registry),
):
    """
    Fetch a slice of array-like data.
    """
    if reader.structure_family != "structured_array_generic":
        raise HTTPException(
            status_code=404,
            detail=f"Cannot read {reader.structure_family} structure with /structured_array_generic/full route.",
        )
    # Deferred import because this is not a required dependency of the server
    # for some use cases.
    import numpy

    try:
        array = reader.read()
        if slice:
            array = array[slice]
        array = numpy.asarray(array)  # Force dask or PIMS or ... to do I/O.
    except IndexError:
        raise HTTPException(status_code=400, detail="Block index out of range")
    if (expected_shape is not None) and (expected_shape != array.shape):
        raise HTTPException(
            status_code=400,
            detail=f"The expected_shape {expected_shape} does not match the actual shape {array.shape}",
        )
    try:
        return construct_data_response(
            "structured_array_generic",
            serialization_registry,
            array,
            reader.metadata,
            request,
            format,
        )
    except UnsupportedMediaTypes as err:
        raise HTTPException(status_code=406, detail=", ".join(err.supported))


@router.get(
    "/dataframe/meta/{path:path}",
    response_model=models.Response,
    name="dataframe meta",
)
def dataframe_meta(
    request: Request,
    reader=Depends(reader),
    format: Optional[str] = None,
    serialization_registry=Depends(get_serialization_registry),
):
    """
    Fetch the Apache Arrow serialization of (an empty) DataFrame with this structure.
    """
    request.state.endpoint = "data"
    if reader.structure_family != "dataframe":
        raise HTTPException(
            status_code=404,
            detail=f"Cannot read {reader.structure_family} structure with /dataframe/meta route.",
        )
    meta = reader.microstructure().meta
    with record_timing(request.state.metrics, "pack"):
        content = serialization_registry(
            "dataframe", APACHE_ARROW_FILE_MIME_TYPE, meta, {}
        )
    headers = {"ETag": md5(content).hexdigest()}
    return PatchedResponse(
        content,
        media_type=APACHE_ARROW_FILE_MIME_TYPE,
        headers=headers,
    )


@router.get(
    "/dataframe/divisions/{path:path}",
    response_model=models.Response,
    name="dataframe divisions",
)
def dataframe_divisions(
    request: Request,
    reader=Depends(reader),
    format: Optional[str] = None,
    serialization_registry=Depends(get_serialization_registry),
):
    """
    Fetch the Apache Arrow serialization of the index values at the partition edges.
    """
    request.state.endpoint = "data"
    if reader.structure_family != "dataframe":
        raise HTTPException(
            status_code=404,
            detail=f"Cannot read {reader.structure_family} structure with /dataframe/division route.",
        )
    import pandas

    divisions = reader.microstructure().divisions
    # divisions is a tuple. Wrap it in a DataFrame so
    # that we can easily serialize it with Arrow in the normal way.
    divisions_wrapped_in_df = pandas.DataFrame({"divisions": list(divisions)})
    with record_timing(request.state.metrics, "pack"):
        content = serialization_registry(
            "dataframe", APACHE_ARROW_FILE_MIME_TYPE, divisions_wrapped_in_df, {}
        )
    headers = {"ETag": md5(content).hexdigest()}
    return PatchedResponse(
        content,
        media_type=APACHE_ARROW_FILE_MIME_TYPE,
        headers=headers,
    )


@router.get(
    "/dataframe/partition/{path:path}",
    response_model=models.Response,
    name="dataframe partition",
)
def dataframe_partition(
    request: Request,
    partition: int,
    reader=Depends(reader),
    column: Optional[List[str]] = Query(None, min_length=1),
    format: Optional[str] = None,
    serialization_registry=Depends(get_serialization_registry),
):
    """
    Fetch a partition (continuous block of rows) from a DataFrame.
    """
    if reader.structure_family != "dataframe":
        raise HTTPException(
            status_code=404,
            detail=f"Cannot read {reader.structure_family} structure with /dataframe/parition route.",
        )
    try:
        # The singular/plural mismatch here of "columns" and "column" is
        # due to the ?column=A&column=B&column=C... encodes in a URL.
        with record_timing(request.state.metrics, "read"):
            df = reader.read_partition(partition, columns=column)
    except IndexError:
        raise HTTPException(status_code=400, detail="Partition out of range")
    except KeyError as err:
        (key,) = err.args
        raise HTTPException(status_code=400, detail=f"No such column {key}.")
    try:
        with record_timing(request.state.metrics, "pack"):
            return construct_data_response(
                "dataframe",
                serialization_registry,
                df,
                reader.metadata,
                request,
                format,
            )
    except UnsupportedMediaTypes as err:
        raise HTTPException(status_code=406, detail=", ".join(err.supported))


@router.get(
    "/dataframe/full/{path:path}",
    response_model=models.Response,
    name="full dataframe",
)
def dataframe_full(
    request: Request,
    reader=Depends(reader),
    column: Optional[List[str]] = Query(None, min_length=1),
    format: Optional[str] = None,
    serialization_registry=Depends(get_serialization_registry),
):
    """
    Fetch all the rows of DataFrame.
    """
    if reader.structure_family != "dataframe":
        raise HTTPException(
            status_code=404,
            detail=f"Cannot read {reader.structure_family} structure with /dataframe/full route.",
        )

    specs = getattr(reader, "specs", [])

    try:
        # The singular/plural mismatch here of "columns" and "column" is
        # due to the ?column=A&column=B&column=C... encodes in a URL.
        with record_timing(request.state.metrics, "read"):
            df = reader.read(columns=column)
    except KeyError as err:
        (key,) = err.args
        raise HTTPException(status_code=400, detail=f"No such column {key}.")
    try:
        with record_timing(request.state.metrics, "pack"):
            return construct_data_response(
                "dataframe",
                serialization_registry,
                df,
                reader.metadata,
                request,
                format,
                specs,
            )
    except UnsupportedMediaTypes as err:
        raise HTTPException(status_code=406, detail=", ".join(err.supported))


@router.get(
    "/variable/block/{path:path}",
    response_model=models.Response,
    name="xarray.Variable block",
)
def variable_block(
    request: Request,
    reader=Depends(reader),
    block=Depends(block),
    slice=Depends(slice_),
    expected_shape=Depends(expected_shape),
    format: Optional[str] = None,
    serialization_registry=Depends(get_serialization_registry),
):
    """
    Fetch a chunk of array-like data from an xarray.Variable.
    """
    if reader.structure_family != "variable":
        raise HTTPException(
            status_code=404,
            detail=f"Cannot read {reader.structure_family} structure with /variable/block route.",
        )
    try:
        # Lookup block on the `data` attribute of the Variable.
        with record_timing(request.state.metrics, "read"):
            array = reader.read_block(block, slice=slice)
        if slice:
            array = array[slice]
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
                reader.metadata,
                request,
                format,
            )
    except UnsupportedMediaTypes as err:
        raise HTTPException(status_code=406, detail=", ".join(err.supported))


@router.get(
    "/variable/full/{path:path}",
    response_model=models.Response,
    name="full xarray.Variable",
)
def variable_full(
    request: Request,
    reader=Depends(reader),
    expected_shape=Depends(expected_shape),
    format: Optional[str] = None,
    serialization_registry=Depends(get_serialization_registry),
):
    """
    Fetch a full xarray.Variable.
    """
    if reader.structure_family != "variable":
        raise HTTPException(
            status_code=404,
            detail=f"Cannot read {reader.structure_family} structure with /variable/full route.",
        )
    with record_timing(request.state.metrics, "read"):
        array = reader.read()
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
                reader.metadata,
                request,
                format,
            )
    except UnsupportedMediaTypes as err:
        raise HTTPException(status_code=406, detail=", ".join(err.supported))


@router.get(
    "/data_array/variable/full/{path:path}",
    response_model=models.Response,
    name="full xarray.Variable from within an xarray.DataArray",
)
def data_array_variable_full(
    request: Request,
    reader=Depends(reader),
    coord: Optional[str] = Query(None, min_length=1),
    expected_shape=Depends(expected_shape),
    format: Optional[str] = None,
    serialization_registry=Depends(get_serialization_registry),
):
    """
    Fetch a chunk from an xarray.DataArray.
    """
    if reader.structure_family != "data_array":
        raise HTTPException(
            status_code=404,
            detail=f"Cannot read {reader.structure_family} structure with /data_array/variable/full route.",
        )
    # TODO Should read() accept a `coord` argument?
    with record_timing(request.state.metrics, "read"):
        array = reader.read()
    if coord is not None:
        try:
            array = array.coords[coord]
        except KeyError:
            raise HTTPException(
                status_code=400,
                detail=f"No such coordinate {coord}.",
            )
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
                reader.metadata,
                request,
                format,
            )
    except UnsupportedMediaTypes as err:
        raise HTTPException(status_code=406, detail=", ".join(err.supported))


@router.get(
    "/data_array/block/{path:path}",
    response_model=models.Response,
    name="xarray.DataArray block",
)
def data_array_block(
    request: Request,
    reader=Depends(reader),
    block=Depends(block),
    coord: Optional[str] = Query(None, min_length=1),
    slice=Depends(slice_),
    expected_shape=Depends(expected_shape),
    format: Optional[str] = None,
    serialization_registry=Depends(get_serialization_registry),
):
    """
    Fetch a chunk from an xarray.DataArray.
    """
    if reader.structure_family != "data_array":
        raise HTTPException(
            status_code=404,
            detail=f"Cannot read {reader.structure_family} structure with /data_array/block route.",
        )
    try:
        with record_timing(request.state.metrics, "read"):
            array = reader.read_block(block, coord, slice=slice)
    except IndexError:
        raise HTTPException(status_code=400, detail="Block index out of range")
    except KeyError:
        if coord is not None:
            raise HTTPException(
                status_code=400,
                detail=f"No such coordinate {coord}.",
            )
        else:
            raise
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
                reader.metadata,
                request,
                format,
            )
    except UnsupportedMediaTypes as err:
        raise HTTPException(status_code=406, detail=", ".join(err.supported))


@router.get(
    "/dataset/block/{path:path}",
    response_model=models.Response,
    name="xarray.Dataset block",
)
def dataset_block(
    request: Request,
    reader=Depends(reader),
    block=Depends(block),
    variable: Optional[str] = Query(None, min_length=1),
    coord: Optional[str] = Query(None, min_length=1),
    slice=Depends(slice_),
    expected_shape=Depends(expected_shape),
    format: Optional[str] = None,
    serialization_registry=Depends(get_serialization_registry),
):
    """
    Fetch a chunk from an xarray.Dataset.
    """
    if reader.structure_family != "dataset":
        raise HTTPException(
            status_code=404,
            detail=f"Cannot read {reader.structure_family} structure with /dataset/block route.",
        )
    try:
        with record_timing(request.state.metrics, "read"):
            array = reader.read_block(variable, block, coord, slice=slice)
    except IndexError:
        raise HTTPException(status_code=400, detail="Block index out of range")
    except KeyError:
        if coord is None:
            raise HTTPException(
                status_code=400,
                detail=f"No such variable {variable}.",
            )
        if variable is None:
            raise HTTPException(
                status_code=400,
                detail=f"No such coordinate {coord}.",
            )
        raise HTTPException(
            status_code=400,
            detail=f"No such coordinate {coord} and/or variable {variable}.",
        )
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
                reader.metadata,
                request,
                format,
            )
    except UnsupportedMediaTypes as err:
        raise HTTPException(status_code=406, detail=", ".join(err.supported))


@router.get(
    "/dataset/data_var/full/{path:path}",
    response_model=models.Response,
    name="full xarray.Dataset data variable",
)
def dataset_data_var_full(
    request: Request,
    reader=Depends(reader),
    variable: str = Query(..., min_length=1),
    coord: Optional[str] = Query(None, min_length=1),
    expected_shape=Depends(expected_shape),
    format: Optional[str] = None,
    serialization_registry=Depends(get_serialization_registry),
):
    """
    Fetch a full xarray.Variable from within an xarray.Dataset.
    """
    if reader.structure_family != "dataset":
        raise HTTPException(
            status_code=404,
            detail=f"Cannot read {reader.structure_family} structure with /dataset/data_var/full route.",
        )
    try:
        with record_timing(request.state.metrics, "read"):
            array = reader.read_variable(variable).data
    except KeyError:
        raise HTTPException(
            status_code=400,
            detail=f"No such variable {variable}.",
        )
    if coord is not None:
        try:
            array = array.coords[coord].data
        except KeyError:
            raise HTTPException(
                status_code=400,
                detail=f"No such coordinate {coord}.",
            )
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
                reader.metadata,
                request,
                format,
            )
    except UnsupportedMediaTypes as err:
        raise HTTPException(status_code=406, detail=", ".join(err.supported))


@router.get(
    "/dataset/coord/full/{path:path}",
    response_model=models.Response,
    name="full xarray.Dataset coordinate",
)
def dataset_coord_full(
    request: Request,
    reader=Depends(reader),
    coord: str = Query(..., min_length=1),
    expected_shape=Depends(expected_shape),
    format: Optional[str] = None,
    serialization_registry=Depends(get_serialization_registry),
):
    """
    Fetch a full coordinate from within an xarray.Dataset.
    """
    if reader.structure_family != "dataset":
        raise HTTPException(
            status_code=404,
            detail=f"Cannot read {reader.structure_family} structure with /dataset/coord/full route.",
        )
    try:
        with record_timing(request.state.metrics, "read"):
            array = reader.read_variable(coord).data
    except KeyError:
        raise HTTPException(
            status_code=400,
            detail=f"No such coordinate {coord}.",
        )
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
                reader.metadata,
                request,
                format,
            )
    except UnsupportedMediaTypes as err:
        raise HTTPException(status_code=406, detail=", ".join(err.supported))


@router.get(
    "/dataset/full/{path:path}",
    response_model=models.Response,
    name="full xarray.Dataset",
)
def dataset_full(
    request: Request,
    reader=Depends(reader),
    variable: Optional[List[str]] = Query(None, min_length=1),
    format: Optional[str] = None,
    serialization_registry=Depends(get_serialization_registry),
):
    """
    Fetch a full coordinate from within an xarray.Dataset.
    """
    if reader.structure_family != "dataset":
        raise HTTPException(
            status_code=404,
            detail=f"Cannot read {reader.structure_family} structure with /dataset/full route.",
        )
    try:
        # The singular/plural mismatch here of "variables" and "variable" is
        # due to the ?variable=A&variable=B&variable=C... encodes in a URL.
        with record_timing(request.state.metrics, "read"):
            dataset = reader.read(variables=variable)
    except KeyError as err:
        (key,) = err.args
        raise HTTPException(status_code=400, detail=f"No such variable {key}.")
    try:
        with record_timing(request.state.metrics, "pack"):
            return construct_data_response(
                "dataset",
                serialization_registry,
                dataset,
                reader.metadata,
                request,
                format,
            )
    except UnsupportedMediaTypes as err:
        breakpoint()
        raise HTTPException(status_code=406, detail=", ".join(err.supported))


def _get_base_url(request):
    # Confusing thing:
    # An httpx.URL treats netloc as bytes (in 0.18.2)
    # but starlette.datastructures.URL treats netloc as str.
    # It seems possible starlette could change their minds in
    # the future to align with httpx, so we will accept either
    # str or bytes here.
    client_specified_base_url = request.headers.get("x-base-url")
    if client_specified_base_url is not None:
        return client_specified_base_url
    url = request.url
    root_path = request.scope.get("root_path") or "/"
    if isinstance(url.netloc, bytes):
        netloc_str = url.netloc.decode()
    else:
        netloc_str = url.netloc
    return f"{url.scheme}://{netloc_str}{root_path}"
