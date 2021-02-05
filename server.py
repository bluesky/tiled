from ast import literal_eval
from operator import length_hint
import os
from typing import Optional
from fastapi import FastAPI, HTTPException, Query, Response
from msgpack_asgi import MessagePackMiddleware

from server_utils import (
    construct_response_data_from_items,
    get_entry,
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

    catalog = get_entry(path)
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

    catalog = get_entry(path)
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
async def list_description(
    path: Optional[str],
    offset: Optional[int] = Query(0, alias="page[offset]"),
    limit: Optional[int] = Query(10, alias="page[limit]"),
):
    catalog = get_entry(path)
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


@app.get("/datasource/description")
@app.get("/datasource/description/{path:path}")
async def one_description(
    path: Optional[str],
    offset: Optional[int] = Query(0, alias="page[offset]"),
    limit: Optional[int] = Query(10, alias="page[limit]"),
):
    datasource = get_entry(path)
    # Take the response we build for /entries and augment it.
    *_, key = path.rsplit("/", 1)
    data = construct_response_data_from_items(path, [(key, datasource)], describe=True)

    response = {
        "data": data["datasources"][0],
        # "links": links,
        # "meta": {"count": approx_len},
    }
    return response


@app.get("/datasource/blob/{path:path}")
async def blob(
    path: str,
    blocks: str,  # This is expected to be a list, like "[0,0]".
):
    # Validate request syntax.
    try:
        parsed_blocks = literal_eval(blocks)
    except Exception:
        raise HTTPException(status_code=400, detail=f"Could not parse {blocks}")
    else:
        if (
            not isinstance(parsed_blocks, (tuple, list)) or
            not all(map(lambda x: isinstance(x, int), parsed_blocks))
            ):
                raise HTTPException(status_code=400, detail=f"Could not parse {blocks} as an index")

    datasource = get_entry(path)
    try:
        chunk_bytes = datasource.read().blocks[parsed_blocks].compute().tobytes()
    except IndexError:
        raise HTTPException(status_code=422, detail="Block index out of range")
    return Response(content=chunk_bytes, media_type="application/octet-stream")


# After defining all routes, wrap app with middleware.

# Add support for msgpack-encoded requests/responses as alternative to JSON.
# https://fastapi.tiangolo.com/advanced/middleware/
# https://github.com/florimondmanca/msgpack-asgi
if not os.getenv("DISABLE_MSGPACK_MIDDLEWARE"):
    app = MessagePackMiddleware(app)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
