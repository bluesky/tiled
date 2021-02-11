import os
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
from .queries import queries_by_name
from . import models


app = FastAPI()


@app.post("/search/{path:path}")
@app.post("/search/", include_in_schema=False)
async def search(
    queries: Optional[List[models.LabeledCatalogQuery]],
    path: Optional[str] = "",
    fields: Optional[List[models.EntryFields]] = Query(list(models.EntryFields)),
    offset: Optional[int] = Query(0, alias="page[offset]"),
    limit: Optional[int] = Query(10, alias="page[limit]"),
):
    return construct_entries_response(
        path,
        offset,
        limit,
        fields,
        queries=queries,
    )


@app.on_event("startup")
async def startup_event():
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
        queries=None,
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
    queries,
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
    if queries:
        for catalog_query in queries:
            query = queries_by_name[catalog_query.query_type](**catalog_query.query)
            catalog = catalog.search(query)
    links = pagination_links(offset, limit, len_or_approx(catalog))
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
    return models.Response(data=data, links=links)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
