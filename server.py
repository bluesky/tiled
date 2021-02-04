from operator import length_hint
import os
from typing import Optional
from fastapi import Depends, FastAPI, Query, Request
from msgpack_asgi import MessagePackMiddleware

from server_utils import (
    construct_response_data_from_items,
    get_subcatalog,
    pagination_links,
)

app = FastAPI()

@app.get("/catalogs/keys")
@app.get("/catalogs/keys/{path:path}")
async def keys(
    path: Optional[str],
    offset: Optional[int] = Query(0, alias="page[offset]"),
    limit: Optional[int] = Query(10, alias="page[limit]"),
):

    catalog = get_subcatalog(path)
    approx_len = length_hint(catalog)
    links = pagination_links(offset, limit, approx_len)
    response = {
        "data": list(catalog.index[offset : offset + limit]),
        "links": links,
        "meta": {"count": approx_len},
    }
    return response


@app.get("/catalogs/entries")
@app.get("/catalogs/entries/{path:path}")
async def entries(
    path: Optional[str],
    offset: Optional[int] = Query(0, alias="page[offset]"),
    limit: Optional[int] = Query(10, alias="page[limit]"),
):

    catalog = get_subcatalog(path)
    approx_len = length_hint(catalog)
    links = pagination_links(offset, limit, approx_len)
    items = catalog.index[offset : offset + limit].items()
    data = construct_response_data_from_items(path, items, describe=False)
    response = {
        "data": data,
        "links": links,
        "meta": {"count": approx_len},
    }
    return response


@app.get("/catalogs/description")
@app.get("/catalogs/description/{path:path}")
async def description(
    path: Optional[str],
    offset: Optional[int] = Query(0, alias="page[offset]"),
    limit: Optional[int] = Query(10, alias="page[limit]"),
):
    catalog = get_subcatalog(path)
    approx_len = length_hint(catalog)
    links = pagination_links(offset, limit, approx_len)
    items = catalog.index[offset : offset + limit].items()
    # Take the response we build for /entries and augment it.
    data = construct_response_data_from_items(path, items, describe=True)

    response = {
        "data": data,
        "links": links,
        "meta": {"count": approx_len},
    }
    return response

# After defining all routes, wrap app with middleware.

# Add support for msgpack-encoded requests/responses as alternative to JSON.
# https://fastapi.tiangolo.com/advanced/middleware/
# https://github.com/florimondmanca/msgpack-asgi 
if not os.getenv("DISABLE_MSGPACK_MIDDLEWARE"):
    app = MessagePackMiddleware(app)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
