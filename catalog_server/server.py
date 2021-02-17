from collections import defaultdict
import dataclasses
import inspect
import os
import re
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query, Request, Response
from msgpack_asgi import MessagePackMiddleware

from .server_utils import (
    array_media_types,
    DuckCatalog,
    get_dask_client,
    get_entry,
    get_chunk,
    len_or_approx,
    pagination_links,
    serialize_array,
)
from . import queries  # This is not used, but it registers queries on import.
from .query_registration import name_to_query_type
from . import models


del queries

app = FastAPI()


def declare_search_route(app=app):
    """
    This is done dynamically at app startup.

    We check the registry of known search query types, which is user
    configurable, and use that to define the allowed HTTP query parameters for
    this route.
    """

    # The parameter `app` is passed in so that we bind to the global `app`
    # defined *above* and not the middleware wrapper that overlaods that name
    # below.
    async def search(
        path: Optional[str] = "",
        fields: Optional[List[models.EntryFields]] = Query(list(models.EntryFields)),
        offset: Optional[int] = Query(0, alias="page[offset]"),
        limit: Optional[int] = Query(10, alias="page[limit]"),
        **filters,
    ):
        return construct_entries_response(
            path,
            offset,
            limit,
            fields,
            filters=filters,
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
    app.get("/search/{path:path}")(search)
    app.get("/search", include_in_schema=False)(search)


_FILTER_PARAM_PATTERN = re.compile(r"filter___(?P<name>.*)___(?P<field>[^\d\W][\w\d]+)")


@app.on_event("startup")
async def startup_event():
    declare_search_route()
    # Warm up the dask.distributed Cluster.
    get_dask_client()


@app.on_event("shutdown")
async def shutdown_event():
    "Gracefully shutdown the dask.distributed Client."
    client = get_dask_client()
    await client.close()


@app.get("/metadata/{path:path}")
@app.get("/metadata", include_in_schema=False)
async def metadata(
    path: Optional[str] = "",
    fields: Optional[List[models.EntryFields]] = Query(list(models.EntryFields)),
):
    "Fetch the metadata for one Catalog or Data Source."

    path = path.rstrip("/")
    *_, key = path.rpartition("/")
    try:
        entry = get_entry(path)
    except KeyError:
        raise HTTPException(status_code=404, detail="No such entry.")
    resource = construct_resource(key, entry, fields)
    return models.Response(data=resource)


@app.get("/entries/{path:path}")
@app.get("/entries", include_in_schema=False)
async def entries(
    path: Optional[str] = "",
    offset: Optional[int] = Query(0, alias="page[offset]"),
    limit: Optional[int] = Query(10, alias="page[limit]"),
    fields: Optional[List[models.EntryFields]] = Query(list(models.EntryFields)),
):
    "List the entries in a Catalog, which may be sub-Catalogs or DataSources."

    return construct_entries_response(
        path,
        offset,
        limit,
        fields,
        filters={},
    )


@app.get("/blob/array/{path:path}")
async def blob_array(
    request: Request,
    path: str,
    # TODO How can we make Query a required parameter (no default value) while
    # still applying regex? It seems that using Query makes this parameter
    # optional, and it's not clear how to get around that.
    block: str = Query(None, min_length=1, regex="^[0-9](,[0-9])*$"),
):
    "Provide one block (chunk) of an array."
    try:
        datasource = get_entry(path)
    except KeyError:
        raise HTTPException(status_code=404, detail="No such entry.")
    parsed_block = tuple(map(int, block.split(",")))
    try:
        chunk = datasource.read().blocks[parsed_block]
    except IndexError:
        raise HTTPException(status_code=422, detail="Block index out of range")
    array = await get_chunk(chunk)
    media_types = request.headers.get("Accept", "application/octet-stream")
    for media_type in media_types.split(", "):
        if media_type == "*/*":
            media_type = "application/octet-stream"
        if media_type in array_media_types:
            content = await serialize_array(media_type, array)
            return Response(content=content, media_type=media_type)
    else:
        # We do not support any of the media types requested by the client.
        # Reply with a list of the supported types.
        raise HTTPException(status_code=406, detail=", ".join(array_media_types))


# After defining all routes, wrap app with middleware.

# Add support for msgpack-encoded requests/responses as alternative to JSON.
# https://fastapi.tiangolo.com/advanced/middleware/
# https://github.com/florimondmanca/msgpack-asgi
if not os.getenv("DISABLE_MSGPACK_MIDDLEWARE"):
    app = MessagePackMiddleware(app)


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
                "meta": {
                    "__module__": getattr(type(entry), "__module__"),
                    "__qualname__": getattr(type(entry), "__qualname__"),
                },
            }
        )
    else:
        if models.EntryFields.structure in fields:
            attributes["structure"] = entry.describe()
            attributes["container"] = entry.container
        resource = models.DataSourceResource(
            **{
                "id": key,
                "attributes": models.DataSourceAttributes(**attributes),
                "type": models.EntryType.datasource,
                "meta": {
                    "__module__": getattr(type(entry), "__module__"),
                    "__qualname__": getattr(type(entry), "__qualname__"),
                },
            }
        )
    return resource


def construct_entries_response(
    path,
    offset,
    limit,
    fields,
    filters,
):
    path = path.rstrip("/")
    try:
        catalog = get_entry(path)
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


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
