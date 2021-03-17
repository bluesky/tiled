import dataclasses
import inspect
from typing import Any, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response

from ..query_registration import name_to_query_type
from .authentication import (
    get_current_user,
    get_user_for_token,
    new_token,
    revoke_token,
)
from .core import (
    APACHE_ARROW_FILE_MIME_TYPE,
    block,
    construct_array_response,
    construct_dataframe_response,
    construct_entries_response,
    construct_resource,
    reader,
    entry,
    # get_dask_client,
    json_or_msgpack,
    NoEntry,
    serialization_registry,
    slice_,
    WrongTypeForRoute,
    UnsupportedMediaTypes,
)
from . import models
from .. import __version__


router = APIRouter()


@router.get("/", response_model=models.About)
async def about(request: Request):
    # TODO The lazy import of reader modules and serializers means that the
    # lists of formats are not populated until they are first used. Not very
    # helpful for discovery! The registration can be made non-lazy, while the
    # imports of the underlying I/O libraries themselves (openpyxl, pillow,
    # etc.) can remain lazy.
    return json_or_msgpack(
        request.headers,
        models.About(
            library_version=__version__,
            api_version=0,
            formats={
                container: list(serialization_registry.media_types(container))
                for container in serialization_registry.containers
            },
            queries=list(name_to_query_type),
            # documentation_url=".../docs",  # TODO How to get the base URL?
        ),
    )


@router.post("/token", response_model=models.Token)
async def create_token(username: str, current_user=Depends(get_current_user)):
    "Generate an API access token."
    if (username != current_user) and (current_user != "admin"):
        raise HTTPException(
            status_code=403, detail="Only admin can generate tokens for other users."
        )
    return {"access_token": new_token(username), "token_type": "bearer"}


@router.delete("/token")
async def delete_token(token: models.Token, current_user=Depends(get_current_user)):
    "Generate an API access token."
    username = get_user_for_token(token.access_token)
    if (username != current_user) and (current_user != "admin"):
        raise HTTPException(
            status_code=403, detail="Only admin can delete other users' tokens."
        )
    revoke_token(token.access_token)
    return


def declare_search_route(router):
    """
    This is done dynamically at router startup.

    We check the registry of known search query types, which is user
    configurable, and use that to define the allowed HTTP query parameters for
    this route.
    """

    async def search(
        request: Request,
        path: Optional[str] = "/",
        fields: Optional[List[models.EntryFields]] = Query(list(models.EntryFields)),
        offset: Optional[int] = Query(0, alias="page[offset]"),
        limit: Optional[int] = Query(10, alias="page[limit]"),
        current_user=Depends(get_current_user),
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
                    current_user,
                ),
            )
        except NoEntry:
            raise HTTPException(status_code=404, detail="No such entry.")
        except WrongTypeForRoute as err:
            raise HTTPException(status_code=404, detail=err.args[0])
        except UnsupportedMediaTypes as err:
            # TODO Should we just serve a default representation instead of
            # returning this error code?
            raise HTTPException(status_code=406, detail=", ".join(err.supported))

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
            injected_parameter = inspect.Parameter(
                name=f"filter___{name}___{field.name}",
                kind=inspect.Parameter.POSITIONAL_OR_KEYWORD,
                default=Query(None, alias=f"filter[{name}][condition][{field.name}]"),
                annotation=Optional[field.type],
            )
        parameters.append(injected_parameter)
    search.__signature__ = signature.replace(parameters=parameters)
    # End black magic

    # Register the search route.
    router.get("/search", response_model=models.Response, include_in_schema=False)(
        search
    )
    router.get("/search/{path:path}", response_model=models.Response)(search)


@router.get("/metadata", response_model=models.Response, include_in_schema=False)
@router.get("/metadata/{path:path}", response_model=models.Response)
async def metadata(
    request: Request,
    path: Optional[str] = "/",
    fields: Optional[List[models.EntryFields]] = Query(list(models.EntryFields)),
    current_user=Depends(get_current_user),
    entry: Any = Depends(entry),
):
    "Fetch the metadata for one Catalog or Reader."

    path = path.rstrip("/")
    *_, key = path.rpartition("/")
    resource = construct_resource(key, entry, fields)
    return json_or_msgpack(request.headers, models.Response(data=resource))


@router.get("/entries", response_model=models.Response, include_in_schema=False)
@router.get("/entries/{path:path}", response_model=models.Response)
async def entries(
    request: Request,
    path: Optional[str] = "/",
    offset: Optional[int] = Query(0, alias="page[offset]"),
    limit: Optional[int] = Query(10, alias="page[limit]"),
    fields: Optional[List[models.EntryFields]] = Query(list(models.EntryFields)),
    current_user=Depends(get_current_user),
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
                current_user,
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
    format: Optional[str] = None,
):
    """
    Fetch a chunk of array-like data.
    """
    try:
        array = reader.read_block(block, slice=slice)
    except IndexError:
        raise HTTPException(status_code=422, detail="Block index out of range")
    try:
        return construct_array_response(array, request.headers, format)
    except UnsupportedMediaTypes as err:
        # TODO Should we just serve a default representation instead of
        # returning this error code?
        raise HTTPException(status_code=406, detail=", ".join(err.supported))


@router.get(
    "/array/full/{path:path}", response_model=models.Response, name="full array"
)
def array_full(
    request: Request,
    reader=Depends(reader),
    slice=Depends(slice_),
    format: Optional[str] = None,
):
    """
    Fetch a slice of array-like data.
    """
    # Deferred import because this is not a required dependency of the server
    # for some use cases.
    import numpy

    try:
        array = reader.read()
        if slice:
            array = array[slice]
        array = numpy.asarray(array)  # Force dask or PIMS or ... to do I/O.
    except IndexError:
        raise HTTPException(status_code=422, detail="Block index out of range")
    try:
        return construct_array_response(array, request.headers, format)
    except UnsupportedMediaTypes as err:
        # TODO Should we just serve a default representation instead of
        # returning this error code?
        raise HTTPException(status_code=406, detail=", ".join(err.supported))


@router.get(
    "/dataframe/schema/{path:path}",
    response_model=models.Response,
    name="dataframe partition",
)
def dataframe_schema(
    request: Request,
    partition: int,
    reader=Depends(reader),
    columns: Optional[str] = None,
    format: Optional[str] = None,
):
    """
    Fetch the Apache Arrow serialization of (an empty) DataFrame with this structure.
    """
    import pyarrow

    meta = reader.structure()["meta"]
    return Response(pyarrow.serialize(meta), content_type=APACHE_ARROW_FILE_MIME_TYPE)


@router.get(
    "/dataframe/partition/{path:path}",
    response_model=models.Response,
    name="dataframe partition",
)
def dataframe_partition(
    request: Request,
    partition: int,
    reader=Depends(reader),
    columns: Optional[str] = None,
    format: Optional[str] = None,
):
    """
    Fetch a chunk from an xarray.Dataset.
    """
    try:
        df = reader.read_partition(partition, columns=columns)
    except IndexError:
        raise HTTPException(status_code=422, detail="Partition out of range")
    except KeyError as err:
        (key,) = err.args
        raise HTTPException(status_code=422, detail=f"No such column {key}.")
    try:
        return construct_dataframe_response(df, request.headers, format)
    except UnsupportedMediaTypes as err:
        # TODO Should we just serve a default representation instead of
        # returning this error code?
        raise HTTPException(status_code=406, detail=", ".join(err.supported))


@router.get(
    "/variable/block/{path:path}", response_model=models.Response, name="variable block"
)
def variable_block(
    request: Request,
    reader=Depends(reader),
    block=Depends(block),
    slice=Depends(slice_),
    format: Optional[str] = None,
):
    """
    Fetch a chunk of array-like data.
    """
    try:
        # Lookup block on the `data` attribute of the Variable.
        array = reader.read_block(block, slice=slice)
        if slice:
            array = array[slice]
    except IndexError:
        raise HTTPException(status_code=422, detail="Block index out of range")
    try:
        return construct_array_response(array, request.headers, format)
    except UnsupportedMediaTypes as err:
        # TODO Should we just serve a default representation instead of
        # returning this error code?
        raise HTTPException(status_code=406, detail=", ".join(err.supported))


@router.get(
    "/data_array/block/{path:path}", response_model=models.Response, name="data_array"
)
def data_array_block(
    request: Request,
    reader=Depends(reader),
    block=Depends(block),
    coord: Optional[str] = Query(None, min_length=1),
    slice=Depends(slice_),
    format: Optional[str] = None,
):
    """
    Fetch a chunk from an xarray.DataArray.
    """
    try:
        array = reader.read_block(block, coord, slice=slice)
    except IndexError:
        raise HTTPException(status_code=422, detail="Block index out of range")
    except KeyError:
        if coord is not None:
            raise HTTPException(
                status_code=422,
                detail=f"No such coordinate {coord}.",
            )
        else:
            raise
    try:
        return construct_array_response(array, request.headers, format)
    except UnsupportedMediaTypes as err:
        # TODO Should we just serve a default representation instead of
        # returning this error code?
        raise HTTPException(status_code=406, detail=", ".join(err.supported))


@router.get(
    "/dataset/block/{path:path}", response_model=models.Response, name="dataset"
)
def dataset_block(
    request: Request,
    reader=Depends(reader),
    block=Depends(block),
    variable: str = Query(..., min_length=1),
    coord: Optional[str] = Query(None, min_length=1),
    slice=Depends(slice_),
    format: Optional[str] = None,
):
    """
    Fetch a chunk from an xarray.Dataset.
    """
    try:
        array = reader.read_block(variable, block, coord, slice=slice)
    except IndexError:
        raise HTTPException(status_code=422, detail="Block index out of range")
    except KeyError:
        if coord is None:
            raise HTTPException(
                status_code=422,
                detail=f"No such variable {variable}.",
            )
        raise HTTPException(
            status_code=422,
            detail=f"No such coordinate {coord}.",
        )
    try:
        return construct_array_response(array, request.headers, format)
    except UnsupportedMediaTypes as err:
        # TODO Should we just serve a default representation instead of
        # returning this error code?
        raise HTTPException(status_code=406, detail=", ".join(err.supported))
