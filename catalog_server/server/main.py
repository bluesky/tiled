from collections import defaultdict
import dataclasses
import inspect
import os
import re
from typing import Any, List, Optional

import dask.base
from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response
from msgpack_asgi import MessagePackMiddleware

from .utils import (
    DuckCatalog,
    get_chunk,
    # get_dask_client,
    get_entry,
    get_settings,
    len_or_approx,
    pagination_links,
)
from . import models
from .authentication import (
    get_current_user,
    get_user_for_token,
    new_token,
    revoke_token,
)
from .. import queries  # This is not used, but it registers queries on import.
from ..media_type_registration import serialization_registry
from ..query_registration import name_to_query_type

del queries


api = FastAPI()


@api.post("/token", response_model=models.Token)
async def create_token(username: str, current_user=Depends(get_current_user)):
    "Generate an API access token."
    if (username != current_user) and (current_user != "admin"):
        raise HTTPException(
            status_code=403, detail="Only admin can generate tokens for other users."
        )
    return {"access_token": new_token(username), "token_type": "bearer"}


@api.delete("/token")
async def delete_token(token: models.Token, current_user=Depends(get_current_user)):
    "Generate an API access token."
    username = get_user_for_token(token.access_token)
    if (username != current_user) and (current_user != "admin"):
        raise HTTPException(
            status_code=403, detail="Only admin can delete other users' tokens."
        )
    revoke_token(token.access_token)
    return


class PatchedResponse(Response):
    "Patch the render method to accept memoryview."

    def render(self, content: Any) -> bytes:
        if isinstance(content, memoryview):
            return content.cast("B")
        return super().render(content)


def declare_search_route():
    """
    This is done dynamically at api startup.

    We check the registry of known search query types, which is user
    configurable, and use that to define the allowed HTTP query parameters for
    this route.
    """

    async def search(
        path: Optional[str] = "/",
        fields: Optional[List[models.EntryFields]] = Query(list(models.EntryFields)),
        offset: Optional[int] = Query(0, alias="page[offset]"),
        limit: Optional[int] = Query(10, alias="page[limit]"),
        current_user=Depends(get_current_user),
        **filters,
    ):
        return construct_entries_response(
            path,
            offset,
            limit,
            fields,
            filters,
            current_user,
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
    signature = inspect.signature(search)
    parameters = list(signature.parameters.values())
    # Drop the **filters parameter from the signature.
    del parameters[-1]
    # Add a parameter for each field in each type of query.
    for name, query in name_to_query_type.items():
        for field in dataclasses.fields(query):
            # The structured "alias" here is based on
            # https://mglaman.dev/blog/using-json-api-query-your-search-api-indexes
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
    api.get("/search/{path:path}", response_model=models.Response)(search)
    api.get("/search", response_model=models.Response, include_in_schema=False)(search)


_FILTER_PARAM_PATTERN = re.compile(r"filter___(?P<name>.*)___(?P<field>[^\d\W][\w\d]+)")


@api.on_event("startup")
async def startup_event():
    declare_search_route()
    # Warm up cached access.
    get_settings().catalog
    # get_dask_client()


@api.on_event("shutdown")
async def shutdown_event():
    # client = get_dask_client()
    # await client.close()
    pass


@api.get("/metadata/{path:path}", response_model=models.Response)
@api.get("/metadata", response_model=models.Response, include_in_schema=False)
async def metadata(
    path: Optional[str] = "/",
    fields: Optional[List[models.EntryFields]] = Query(list(models.EntryFields)),
    current_user=Depends(get_current_user),
):
    "Fetch the metadata for one Catalog or Data Source."

    path = path.rstrip("/")
    *_, key = path.rpartition("/")
    try:
        entry = get_entry(path, current_user)
    except KeyError:
        raise HTTPException(status_code=404, detail="No such entry.")
    resource = construct_resource(key, entry, fields)
    return models.Response(data=resource)


@api.get("/entries/{path:path}", response_model=models.Response)
@api.get("/entries", response_model=models.Response, include_in_schema=False)
async def entries(
    path: Optional[str] = "/",
    offset: Optional[int] = Query(0, alias="page[offset]"),
    limit: Optional[int] = Query(10, alias="page[limit]"),
    fields: Optional[List[models.EntryFields]] = Query(list(models.EntryFields)),
    current_user=Depends(get_current_user),
):
    "List the entries in a Catalog, which may be sub-Catalogs or DataSources."

    return construct_entries_response(
        path,
        offset,
        limit,
        fields,
        {},
        current_user,
    )


def datasource(
    path: str,
    current_user: str = Depends(get_current_user),
):
    "Specify a path parameter and use it to look up a datasource."
    try:
        return get_entry(path, current_user)
    except KeyError:
        raise HTTPException(status_code=404, detail="No such entry.")


def block(
    # Ellipsis as the "default" tells FastAPI to make this parameter required.
    block: str = Query(..., min_length=1, regex="^[0-9](,[0-9])*$"),
):
    "Specify and parse a block index parameter."
    parsed_block = tuple(map(int, block.split(",")))
    return parsed_block


@api.get("/blob/array/{path:path}", response_model=models.Response, name="array")
def blob_array(
    request: Request,
    datasource=Depends(datasource),
    block=Depends(block),
):
    """
    Fetch a chunk of array-like data.
    """
    try:
        chunk = datasource.read().blocks[block]
    except IndexError:
        raise HTTPException(status_code=422, detail="Block index out of range")
    array = get_chunk(chunk)
    return construct_array_response(array, request.headers)


@api.get("/blob/variable/{path:path}", response_model=models.Response, name="variable")
def blob_variable(
    request: Request,
    datasource=Depends(datasource),
    block=Depends(block),
):
    """
    Fetch a chunk of array-like data.
    """
    try:
        # Lookup block on the `data` attribute of the Variable.
        chunk = datasource.read().data.blocks[block]
    except IndexError:
        raise HTTPException(status_code=422, detail="Block index out of range")
    array = get_chunk(chunk)
    return construct_array_response(array, request.headers)


# After defining all routes, wrap api with middleware.
# Add support for msgpack-encoded requests/responses as alternative to JSON.
# https://fastapi.tiangolo.com/advanced/middleware/
# https://github.com/florimondmanca/msgpack-asgi
if not os.getenv("DISABLE_MSGPACK_MIDDLEWARE"):
    app = MessagePackMiddleware(api)
else:
    app = api


def construct_resource(key, entry, fields):
    attributes = {}
    if models.EntryFields.metadata in fields:
        attributes["metadata"] = entry.metadata
    if isinstance(entry, DuckCatalog):
        if models.EntryFields.count in fields:
            attributes["count"] = len_or_approx(entry)
        resource = models.CatalogResource(
            **{
                "id": key,
                "attributes": models.CatalogAttributes(**attributes),
                "type": models.EntryType.catalog,
            }
        )
    else:
        if models.EntryFields.container in fields:
            attributes["container"] = entry.container
        if models.EntryFields.structure in fields:
            attributes["structure"] = dataclasses.asdict(entry.describe())
        resource = models.DataSourceResource(
            **{
                "id": key,
                "attributes": models.DataSourceAttributes(**attributes),
                "type": models.EntryType.datasource,
            }
        )
    return resource


def construct_entries_response(
    path,
    offset,
    limit,
    fields,
    filters,
    current_user,
):
    path = path.rstrip("/")
    try:
        catalog = get_entry(path, current_user)
    except KeyError:
        raise HTTPException(status_code=404, detail="No such entry.")
    if not isinstance(catalog, DuckCatalog):
        raise HTTPException(
            status_code=404, detail="This is a Data Source, not a Catalog."
        )
    queries = defaultdict(
        dict
    )  # e.g. {"text": {"text": "dog"}, "lookup": {"key": "..."}}
    # Group the parameters by query type.
    for key, value in filters.items():
        if value is None:
            continue
        name, field = _FILTER_PARAM_PATTERN.match(key).groups()
        queries[name][field] = value
    # Apply the queries and obtain a narrowed catalog.
    for name, parameters in queries.items():
        query_class = name_to_query_type[name]
        query = query_class(**parameters)
        catalog = catalog.search(query)
    count = len_or_approx(catalog)
    links = pagination_links(offset, limit, count)
    data = []
    if fields:
        # Pull a page of items into memory.
        items = catalog.items_indexer[offset : offset + limit]
    else:
        # Pull a page of just the keys, which is cheaper.
        items = ((key, None) for key in catalog.keys_indexer[offset : offset + limit])
    for key, entry in items:
        resource = construct_resource(key, entry, fields)
        data.append(resource)
    return models.Response(data=data, links=links, meta={"count": count})


def construct_array_response(array, request_headers):
    DEFAULT_MEDIA_TYPE = "application/octet-stream"
    etag = dask.base.tokenize(array)
    if request_headers.get("If-None-Match", "") == etag:
        return Response(status_code=304)
    media_types = request_headers.get("Accept", DEFAULT_MEDIA_TYPE)
    for media_type in media_types.split(", "):
        if media_type == "*/*":
            media_type = DEFAULT_MEDIA_TYPE
        if media_type in serialization_registry.media_types("array"):
            content = serialization_registry("array", media_type, array)
            return PatchedResponse(
                content=content, media_type=media_type, headers={"ETag": etag}
            )
    else:
        # We do not support any of the media types requested by the client.
        # Reply with a list of the supported types.
        raise HTTPException(
            status_code=406,
            detail=", ".join(serialization_registry.media_types("array")),
        )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
