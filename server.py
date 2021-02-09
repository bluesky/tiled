from ast import literal_eval
import os
from typing import List, Optional
from fastapi import FastAPI, HTTPException, Query, Request, Response
from msgpack_asgi import MessagePackMiddleware

from server_utils import (
    DuckCatalog,
    get_dask_client,
    get_entry,
    get_chunk,
    len_or_approx,
    pagination_links,
    serialize_array,
)
from queries import queries_by_name
import models


app = FastAPI()


def add_search_routes(app=app):
    """
    Routes for search are defined at the last moment, just before startup, so
    that custom query types may be registered first.
    """
    # We bind app in a parameter above so that we have a reference to the
    # FastAPI instance itself, not the middleware which shadows it below.
    for name, query_class in queries_by_name.items():

        @app.post(f"/search/{name}/{{path:path}}")
        @app.post(f"/search/{name}", include_in_schema=False)
        async def keys_search_text(
            query: query_class,
            path: Optional[str] = "",
            fields: Optional[List[models.EntryFields]] = Query(
                list(models.EntryFields)
            ),
            offset: Optional[int] = Query(0, alias="page[offset]"),
            limit: Optional[int] = Query(10, alias="page[limit]"),
        ):
            return construct_entries_response(
                path,
                offset,
                limit,
                fields,
                query=query,
            )


@app.on_event("startup")
async def startup_event():
    add_search_routes()
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
        query=None,
    )


@app.get("/blob/array/{path:path}")
async def blob(
    request: Request,
    path: str,
    block: str,  # This is expected to be a list, like "[0,0]".
):
    "Provide one block (chunk) or an array."
    # Validate request syntax.
    try:
        parsed_block = literal_eval(block)
    except Exception:
        raise HTTPException(status_code=400, detail=f"Could not parse {block}")
    else:
        if not isinstance(parsed_block, (tuple, list)) or not all(
            map(lambda x: isinstance(x, int), parsed_block)
        ):
            raise HTTPException(
                status_code=400, detail=f"Could not parse {block} as an index"
            )

    try:
        datasource = get_entry(path)
    except KeyError:
        raise HTTPException(status_code=404, detail="No such entry.")
    try:
        chunk = datasource.read().blocks[parsed_block]
    except IndexError:
        raise HTTPException(status_code=422, detail="Block index out of range")
    array = await get_chunk(chunk)
    media_type = request.headers.get("Accept", "application/octet-stream")
    if media_type == "*/*":
        media_type = "application/octet-stream"
    content = await serialize_array(media_type, array)
    return Response(content=content, media_type=media_type)


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
    query,
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
    if query is not None:
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
