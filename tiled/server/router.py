import dataclasses
from hashlib import md5
import inspect
from typing import Any, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request

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
    construct_dataset_response,
    construct_entries_response,
    construct_resource,
    reader,
    entry,
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
    resource = construct_resource(path, key, entry, fields)
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
    divisions = reader.microstructure().divisions
    content = serialization_registry(
        "dataframe", APACHE_ARROW_FILE_MIME_TYPE, divisions
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
    try:
        # The singular/plural mismatch here of "columns" and "column" is
        # due to the ?column=A&column=B&column=C... encodes in a URL.
        df = reader.read_partition(partition, columns=column)
    except IndexError:
        raise HTTPException(status_code=422, detail="Partition out of range")
    except KeyError as err:
        (key,) = err.args
        raise HTTPException(status_code=422, detail=f"No such column {key}.")
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
    try:
        # The singular/plural mismatch here of "columns" and "column" is
        # due to the ?column=A&column=B&column=C... encodes in a URL.
        df = reader.read(columns=column)
    except KeyError as err:
        (key,) = err.args
        raise HTTPException(status_code=422, detail=f"No such column {key}.")
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
    format: Optional[str] = None,
):
    """
    Fetch a chunk of array-like data from an xarray.Variable.
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
        raise HTTPException(status_code=406, detail=", ".join(err.supported))


@router.get(
    "/variable/full/{path:path}",
    response_model=models.Response,
    name="full xarray.Variable",
)
def variable_full(
    request: Request,
    reader=Depends(reader),
    format: Optional[str] = None,
):
    """
    Fetch a full xarray.Variable.
    """
    array = reader.read()
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
    format: Optional[str] = None,
):
    """
    Fetch a chunk from an xarray.DataArray.
    """
    # TODO Should read() accept a `coord` argument?
    array = reader.read()
    if coord is not None:
        try:
            array = array.coords[coord]
        except KeyError:
            raise HTTPException(
                status_code=422,
                detail=f"No such coordinate {coord}.",
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
    format: Optional[str] = None,
):
    """
    Fetch a full xarray.Variable from within an xarray.Dataset.
    """
    try:
        array = reader.read_variable(variable)
    except KeyError:
        raise HTTPException(
            status_code=422,
            detail=f"No such variable {variable}.",
        )
    if coord is not None:
        try:
            array = array.coords[coord]
        except KeyError:
            raise HTTPException(
                status_code=422,
                detail=f"No such coordinate {coord}.",
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
    format: Optional[str] = None,
):
    """
    Fetch a full coordinate from within an xarray.Dataset.
    """
    try:
        array = reader.read_variable(coord)
    except KeyError:
        raise HTTPException(
            status_code=422,
            detail=f"No such coordinate {coord}.",
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
    try:
        # The singular/plural mismatch here of "variables" and "variable" is
        # due to the ?variable=A&variable=B&variable=C... encodes in a URL.
        dataset = reader.read(variables=variable)
    except KeyError as err:
        (key,) = err.args
        raise HTTPException(status_code=422, detail=f"No such variable {key}.")
    try:
        return construct_dataset_response(dataset, request.headers, format)
    except UnsupportedMediaTypes as err:
        raise HTTPException(status_code=406, detail=", ".join(err.supported))
