import dataclasses
from hashlib import md5
import inspect
from typing import Any, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseSettings

from ..query_registration import name_to_query_type
from .authentication import (
    API_KEY_COOKIE_NAME,
    get_authenticator,
    check_single_user_api_key,
)
from .settings import get_settings

from .core import (
    APACHE_ARROW_FILE_MIME_TYPE,
    block,
    construct_array_response,
    construct_dataframe_response,
    construct_dataset_response,
    construct_entries_response,
    construct_resource,
    reader,
    entry,
    expected_shape,
    json_or_msgpack,
    NoEntry,
    PatchedResponse,
    serialization_registry,
    slice_,
    WrongTypeForRoute,
    UnsupportedMediaTypes,
)
from . import models
from .. import __version__


DEFAULT_PAGE_SIZE = 20


router = APIRouter()


@router.get("/", response_model=models.About)
async def about(
    request: Request,
    has_single_user_api_key: str = Depends(check_single_user_api_key),
    settings: BaseSettings = Depends(get_settings),
    authenticator=Depends(get_authenticator),
    root_path: str = Query(None),
):
    # TODO The lazy import of reader modules and serializers means that the
    # lists of formats are not populated until they are first used. Not very
    # helpful for discovery! The registration can be made non-lazy, while the
    # imports of the underlying I/O libraries themselves (openpyxl, pillow,
    # etc.) can remain lazy.
    if (authenticator is None) and has_single_user_api_key:
        if request.cookies.get(API_KEY_COOKIE_NAME) != settings.single_user_api_key:
            request.state.cookies_to_set.append(
                {"key": API_KEY_COOKIE_NAME, "value": settings.single_user_api_key}
            )
    return json_or_msgpack(
        request.headers,
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
            queries=list(name_to_query_type),
            # documentation_url=".../docs",  # TODO How to get the base URL?
            meta={"root_path": request.scope.get("root_path") or "/"}
            if (root_path is not None)
            else {},
        ),
    )


def declare_search_router():
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
        **filters,
    ):
        try:
            return json_or_msgpack(
                request.headers,
                construct_entries_response(
                    entry,
                    "/search",
                    path,
                    offset,
                    limit,
                    fields,
                    filters,
                    sort,
                    _get_base_url(request.url, request.scope.get("root_path") or "/"),
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
    for name, query in name_to_query_type.items():
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
    "Fetch the metadata for one Catalog or Reader."

    base_url = _get_base_url(request.url, request.scope.get("root_path") or "/")
    path_parts = [segment for segment in path.split("/") if segment]
    resource = construct_resource(base_url, path_parts, entry, fields)
    meta = (
        {"root_path": request.scope.get("root_path") or "/"}
        if (root_path is not None)
        else {}
    )
    return json_or_msgpack(request.headers, models.Response(data=resource, meta=meta))


@router.get("/entries/{path:path}", response_model=models.Response)
async def entries(
    request: Request,
    path: Optional[str],
    offset: Optional[int] = Query(0, alias="page[offset]"),
    limit: Optional[int] = Query(DEFAULT_PAGE_SIZE, alias="page[limit]"),
    sort: Optional[str] = Query(None),
    fields: Optional[List[models.EntryFields]] = Query(list(models.EntryFields)),
    entry: Any = Depends(entry),
):
    "List the entries in a Catalog, which may be sub-Catalogs or Readers."

    try:
        return json_or_msgpack(
            request.headers,
            construct_entries_response(
                entry,
                "/entries",
                path,
                offset,
                limit,
                fields,
                {},
                sort,
                _get_base_url(request.url, request.scope.get("root_path") or "/"),
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
):
    """
    Fetch a chunk of array-like data.
    """
    if reader.structure_family != "array":
        raise HTTPException(
            status_code=404,
            detail=f"Cannot read {reader.structure_family} structure with /array/block route.",
        )
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
        return construct_array_response(array, request.headers, format)
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
        return construct_array_response(array, request.headers, format)
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
):
    """
    Fetch the Apache Arrow serialization of (an empty) DataFrame with this structure.
    """
    if reader.structure_family != "dataframe":
        raise HTTPException(
            status_code=404,
            detail=f"Cannot read {reader.structure_family} structure with /dataframe/meta route.",
        )
    meta = reader.microstructure().meta
    content = serialization_registry("dataframe", APACHE_ARROW_FILE_MIME_TYPE, meta)
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
):
    """
    Fetch the Apache Arrow serialization of the index values at the partition edges.
    """
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
    content = serialization_registry(
        "dataframe", APACHE_ARROW_FILE_MIME_TYPE, divisions_wrapped_in_df
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
        df = reader.read_partition(partition, columns=column)
    except IndexError:
        raise HTTPException(status_code=400, detail="Partition out of range")
    except KeyError as err:
        (key,) = err.args
        raise HTTPException(status_code=400, detail=f"No such column {key}.")
    try:
        return construct_dataframe_response(df, request.headers, format)
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
):
    """
    Fetch all the rows of DataFrame.
    """
    if reader.structure_family != "dataframe":
        raise HTTPException(
            status_code=404,
            detail=f"Cannot read {reader.structure_family} structure with /dataframe/full route.",
        )
    try:
        # The singular/plural mismatch here of "columns" and "column" is
        # due to the ?column=A&column=B&column=C... encodes in a URL.
        df = reader.read(columns=column)
    except KeyError as err:
        (key,) = err.args
        raise HTTPException(status_code=400, detail=f"No such column {key}.")
    try:
        return construct_dataframe_response(df, request.headers, format)
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
        return construct_array_response(array, request.headers, format)
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
):
    """
    Fetch a full xarray.Variable.
    """
    if reader.structure_family != "variable":
        raise HTTPException(
            status_code=404,
            detail=f"Cannot read {reader.structure_family} structure with /variable/full route.",
        )
    array = reader.read()
    if (expected_shape is not None) and (expected_shape != array.shape):
        raise HTTPException(
            status_code=400,
            detail=f"The expected_shape {expected_shape} does not match the actual shape {array.shape}",
        )
    try:
        return construct_array_response(array, request.headers, format)
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
        return construct_array_response(array, request.headers, format)
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
        return construct_array_response(array, request.headers, format)
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
    variable: str = Query(..., min_length=1),
    coord: Optional[str] = Query(None, min_length=1),
    slice=Depends(slice_),
    expected_shape=Depends(expected_shape),
    format: Optional[str] = None,
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
        array = reader.read_block(variable, block, coord, slice=slice)
    except IndexError:
        raise HTTPException(status_code=400, detail="Block index out of range")
    except KeyError:
        if coord is None:
            raise HTTPException(
                status_code=400,
                detail=f"No such variable {variable}.",
            )
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
        return construct_array_response(array, request.headers, format)
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
        array = reader.read_variable(variable)
    except KeyError:
        raise HTTPException(
            status_code=400,
            detail=f"No such variable {variable}.",
        )
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
        return construct_array_response(array, request.headers, format)
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
        array = reader.read_variable(coord)
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
        return construct_array_response(array, request.headers, format)
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
        dataset = reader.read(variables=variable)
    except KeyError as err:
        (key,) = err.args
        raise HTTPException(status_code=400, detail=f"No such variable {key}.")
    try:
        return construct_dataset_response(dataset, request.headers, format)
    except UnsupportedMediaTypes as err:
        raise HTTPException(status_code=406, detail=", ".join(err.supported))


def _get_base_url(url, root_path):
    return f"{url.scheme}://{url.netloc}{root_path}"
