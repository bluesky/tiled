"""
Generate a simplified OpenAPI spec intended for LLM agents.

This spec is produced by FastAPI's own ``get_openapi`` on a filtered subset of
the app's routes, so schema generation, ``$ref`` handling, and component
collection are all delegated to FastAPI — not re-implemented here.

Post-processing is limited to:
- Overriding ``operationId`` with short, LLM-readable names
- Adding ``description`` and ``x-usage-example`` enrichments
- Stripping per-operation ``security`` requirements (auth is injected at
  runtime by the agent framework via the Authorization header or cookie)
"""

import copy

from fastapi.openapi.utils import get_openapi
from fastapi.routing import APIRoute

# ---------------------------------------------------------------------------
# Auth POST paths to include alongside all GET /api/v1 endpoints
# ---------------------------------------------------------------------------
AUTH_POST_PATHS = {
    "/api/v1/auth/session/refresh",
    "/api/v1/auth/session/revoke",
    "/api/v1/auth/apikey",
    "/api/v1/auth/provider/oidc/authorize",
    "/api/v1/auth/provider/oidc/device_code",
    "/api/v1/auth/provider/oidc/token",
}

# ---------------------------------------------------------------------------
# Short, verb-noun operationId overrides keyed by (path, method)
# ---------------------------------------------------------------------------
OPERATION_ID_MAP: dict[tuple[str, str], str] = {
    ("/healthz", "GET"): "CheckHealth",
    ("/api/v1/", "GET"): "GetServerInfo",
    ("/api/v1/search/{path}", "GET"): "SearchNodes",
    ("/api/v1/distinct/{path}", "GET"): "GetDistinctValues",
    ("/api/v1/metadata/{path}", "GET"): "GetMetadata",
    ("/api/v1/array/block/{path}", "GET"): "GetArrayBlock",
    ("/api/v1/array/full/{path}", "GET"): "GetArrayFull",
    ("/api/v1/table/partition/{path}", "GET"): "GetTablePartition",
    ("/api/v1/table/full/{path}", "GET"): "GetTableFull",
    ("/api/v1/container/full/{path}", "GET"): "GetContainerFull",
    ("/api/v1/node/full/{path}", "GET"): "GetNodeFull",
    ("/api/v1/awkward/buffers/{path}", "GET"): "GetAwkwardBuffers",
    ("/api/v1/awkward/full/{path}", "GET"): "GetAwkwardFull",
    ("/api/v1/revisions/{path}", "GET"): "GetRevisions",
    ("/api/v1/asset/bytes/{path}", "GET"): "GetAssetBytes",
    ("/api/v1/asset/manifest/{path}", "GET"): "GetAssetManifest",
    ("/api/v1/auth/principal", "GET"): "ListPrincipals",
    ("/api/v1/auth/principal/{uuid}", "GET"): "GetPrincipal",
    ("/api/v1/auth/apikey", "GET"): "GetCurrentApikeyInfo",
    ("/api/v1/auth/apikey", "POST"): "CreateApikey",
    ("/api/v1/auth/whoami", "GET"): "GetWhoami",
    ("/api/v1/auth/provider/oidc/code", "GET"): "GetOIDCAuthCode",
    ("/api/v1/auth/provider/oidc/authorize", "GET"): "GetOIDCAuthorize",
    ("/api/v1/auth/provider/oidc/authorize", "POST"): "PostOIDCAuthorize",
    ("/api/v1/auth/provider/oidc/device_code", "GET"): "GetOIDCDeviceCode",
    ("/api/v1/auth/provider/oidc/device_code", "POST"): "PostOIDCDeviceCode",
    ("/api/v1/auth/session/refresh", "POST"): "RefreshSession",
    ("/api/v1/auth/session/revoke", "POST"): "RevokeSession",
    ("/api/v1/auth/provider/oidc/token", "POST"): "GetOIDCToken",
    ("/api/v1/metrics", "GET"): "GetMetrics",
}

# ---------------------------------------------------------------------------
# Enriched descriptions and parameter examples for key endpoints
# ---------------------------------------------------------------------------
OPERATION_ENRICHMENTS: dict[str, dict] = {
    "CheckHealth": {
        "description": (
            "Check whether the Tiled server is running and healthy. "
            "Call this first to verify connectivity before making data requests."
        ),
        "x-usage-example": "GET /healthz",
    },
    "GetServerInfo": {
        "description": (
            "Return server metadata including version, authentication requirements, "
            "and available providers. Use this to discover authentication options."
        ),
        "x-usage-example": "GET /api/v1/",
    },
    "SearchNodes": {
        "description": (
            "Search and list nodes (datasets, containers, arrays, tables) within the "
            "Tiled catalog. Supports pagination, sorting, and rich metadata filtering. "
            "Use 'path' to scope the search to a subtree. Omit 'path' or use an empty "
            "string to search from the root. "
            "Filter parameters follow the pattern "
            "filter[<type>][condition][<field>]=<value>."
        ),
        "x-usage-example": (
            "GET /api/v1/search/  "
            "- list from root\n"
            "GET /api/v1/search/experiment/run_001  "
            "- list children of experiment/run_001\n"
            "GET /api/v1/search/?filter[eq][condition][key]=sample&"
            "filter[eq][condition][value]=gold  "
            "- filter by metadata key 'sample' == 'gold'\n"
            "GET /api/v1/search/?filter[fulltext][condition][text]=temperature  "
            "- full-text search across metadata"
        ),
    },
    "GetDistinctValues": {
        "description": (
            "Return the distinct values present for requested metadata keys, structure "
            "families, or specs within a subtree. Useful for building faceted filters "
            "or discovering what values are available before searching."
        ),
        "x-usage-example": (
            "GET /api/v1/distinct/?metadata=sample&counts=true  "
            "- get distinct values and counts for metadata key 'sample'"
        ),
    },
    "GetMetadata": {
        "description": (
            "Fetch metadata and structural information for a single node identified by "
            "its path. Returns metadata dict, structure family (array/table/container), "
            "data shape, dtype, column names (for tables), and navigation links. "
            "Always call this before fetching data to determine the structure family "
            "and available data access routes."
        ),
        "x-usage-example": (
            "GET /api/v1/metadata/experiment/run_001/detector  "
            "- get metadata for a specific node"
        ),
    },
    "GetArrayBlock": {
        "description": (
            "Fetch a specific chunk (block) of an array node. A block is identified by "
            "its integer coordinates along each dimension, comma-separated. Within a "
            "block you can further restrict with a slice. Use GetMetadata first to "
            "learn the array shape and chunk layout."
        ),
        "x-usage-example": (
            "GET /api/v1/array/block/path/to/array?block=0,0&format=application/json  "
            "- fetch block (0,0) as JSON"
        ),
    },
    "GetArrayFull": {
        "description": (
            "Fetch the full contents of an array node, optionally with a slice. "
            "Preferred over GetArrayBlock when you need the entire array or a "
            "contiguous slice across all chunks. Supports numpy-style slice notation."
        ),
        "x-usage-example": (
            "GET /api/v1/array/full/path/to/array?format=application/json  "
            "- fetch entire array\n"
            "GET /api/v1/array/full/path/to/array?slice=0:10&format=application/json  "
            "- fetch first 10 elements"
        ),
    },
    "GetTablePartition": {
        "description": (
            "Fetch a single partition (contiguous block of rows) from a table (DataFrame) "
            "node. Use GetMetadata first to learn how many partitions the table has "
            "(structure.npartitions) and what columns are available."
        ),
        "x-usage-example": (
            "GET /api/v1/table/partition/path/to/table?partition=0&format=application/json  "
            "- fetch first partition\n"
            "GET /api/v1/table/partition/path/to/table?partition=0&column=x&column=y  "
            "- fetch only columns x and y"
        ),
    },
    "GetTableFull": {
        "description": (
            "Fetch all partitions of a table (DataFrame) node concatenated into a single "
            "response. Use for small tables. For large tables prefer GetTablePartition "
            "to page through the data."
        ),
        "x-usage-example": (
            "GET /api/v1/table/full/path/to/table?format=application/json  "
            "- fetch entire table as JSON"
        ),
    },
    "GetContainerFull": {
        "description": (
            "Fetch the full contents of a container node. A container is a nested "
            "collection of other nodes. Returns all child nodes and their data."
        ),
        "x-usage-example": (
            "GET /api/v1/container/full/path/to/container?format=application/json"
        ),
    },
    "GetAwkwardBuffers": {
        "description": (
            "Fetch raw Awkward Array buffers for an awkward-type node. Use this when "
            "working with variable-length or nested array structures. Specify one or "
            "more form_key parameters to select specific buffers."
        ),
        "x-usage-example": (
            "GET /api/v1/awkward/buffers/path/to/node?form_key=node0-offsets"
        ),
    },
    "GetAwkwardFull": {
        "description": (
            "Fetch the full contents of an AwkwardArray node in a portable format."
        ),
        "x-usage-example": (
            "GET /api/v1/awkward/full/path/to/node?format=application/json"
        ),
    },
    "GetRevisions": {
        "description": (
            "List the revision history for a node. Returns a paginated list of "
            "revisions with timestamps and metadata diffs."
        ),
        "x-usage-example": "GET /api/v1/revisions/path/to/node",
    },
    "GetAssetBytes": {
        "description": (
            "Download the raw bytes of an asset associated with a node. "
            "Use GetAssetManifest first to discover available asset IDs."
        ),
        "x-usage-example": "GET /api/v1/asset/bytes/path/to/node?id=1",
    },
    "GetAssetManifest": {
        "description": (
            "List the assets (files) associated with a node, including their IDs, "
            "paths, and sizes. Use asset IDs with GetAssetBytes to download them."
        ),
        "x-usage-example": "GET /api/v1/asset/manifest/path/to/node?id=1",
    },
    "ListPrincipals": {
        "description": "List all principals (users and service accounts) registered with the server.",
        "x-usage-example": "GET /api/v1/auth/principal?page[offset]=0&page[limit]=20",
    },
    "GetPrincipal": {
        "description": "Get detailed information about a single principal by UUID.",
        "x-usage-example": "GET /api/v1/auth/principal/550e8400-e29b-41d4-a716-446655440000",
    },
    "GetCurrentApikeyInfo": {
        "description": (
            "Return metadata about the API key used to authenticate the current request. "
            "Use this to look up the key's UUID given its secret value."
        ),
        "x-usage-example": "GET /api/v1/auth/apikey",
    },
    "GetWhoami": {
        "description": (
            "Return the identity of the currently authenticated principal. "
            "Use this to verify authentication is working and to get your user UUID."
        ),
        "x-usage-example": "GET /api/v1/auth/whoami",
    },
    "GetOIDCAuthorize": {
        "description": "Begin the OIDC authorization code flow. Redirect the user's browser to this URL.",
        "x-usage-example": "GET /api/v1/auth/provider/oidc/authorize",
    },
    "GetOIDCDeviceCode": {
        "description": "Render the device code confirmation form during a device authorization flow.",
        "x-usage-example": "GET /api/v1/auth/provider/oidc/device_code?code=ABCD-1234",
    },
    "GetMetrics": {
        "description": "Return Prometheus-format metrics for server monitoring.",
        "x-usage-example": "GET /api/v1/metrics",
    },
    "RefreshSession": {
        "description": (
            "Exchange a refresh token for a new access token and refresh token pair. "
            "Call this when the access token has expired."
        ),
        "x-usage-example": 'POST /api/v1/auth/session/refresh  body: {"refresh_token": "<token>"}',
    },
    "RevokeSession": {
        "description": "Invalidate a session so its refresh token can no longer be used.",
        "x-usage-example": 'POST /api/v1/auth/session/revoke  body: {"refresh_token": "<token>"}',
    },
    "GetOIDCToken": {
        "description": (
            "Exchange a device authorization grant for access and refresh tokens. "
            "Used in the device code flow after the user has approved the request."
        ),
        "x-usage-example": (
            "POST /api/v1/auth/provider/oidc/token  "
            'body: {"grant_type": "urn:ietf:params:oauth:grant-type:device_code", '
            '"device_code": "<code>"}'
        ),
    },
    "CreateApikey": {
        "description": (
            "Generate a new API key for the currently authenticated user or service. "
            "Returns the secret key value — store it securely as it cannot be retrieved again."
        ),
        "x-usage-example": (
            "POST /api/v1/auth/apikey  "
            'body: {"scopes": ["read:metadata", "read:data"], "expires_in": 86400}'
        ),
    },
    "PostOIDCAuthorize": {
        "description": "Begin the OIDC device authorization flow (POST variant).",
        "x-usage-example": "POST /api/v1/auth/provider/oidc/authorize",
    },
    "PostOIDCDeviceCode": {
        "description": "Submit the user code during the device authorization flow.",
        "x-usage-example": (
            "POST /api/v1/auth/provider/oidc/device_code  "
            'body: {"user_code": "ABCD-1234"}'
        ),
    },
}


def _should_include(route: APIRoute) -> bool:
    """Return True if this route belongs in the agent spec."""
    methods = route.methods or set()
    path = route.path_format
    if "GET" in methods:
        return path.startswith("/api/v1") or path == "/healthz"
    if "POST" in methods:
        return path in AUTH_POST_PATHS
    return False


def build_agent_openapi(app) -> dict:
    """
    Build the agent OpenAPI spec from a FastAPI ``app`` instance.

    Uses FastAPI's own ``get_openapi`` on a filtered route list so that schema
    generation, component collection, and ``$ref`` handling are all delegated
    to FastAPI.  Post-processing adds clean ``operationId``s, enriched
    descriptions, and ``x-usage-example`` fields.

    Parameters
    ----------
    app : FastAPI
        The running application instance (the same one used for the default spec).

    Returns
    -------
    dict
        A valid OpenAPI 3.1.0 schema dict suitable for LLM agent tool use.
    """
    filtered_routes = []
    for route in app.routes:
        if not isinstance(route, APIRoute):
            continue
        if not _should_include(route):
            continue

        # Shallow-copy so we can set a clean operation_id without mutating the
        # live route (which would affect the default /openapi.json).
        r = copy.copy(route)
        method = next(iter(route.methods))
        clean_id = OPERATION_ID_MAP.get((route.path_format, method))
        if clean_id:
            r.operation_id = clean_id
            # unique_id is computed from operation_id at construction time;
            # update it on the copy so get_openapi picks it up.
            r.unique_id = clean_id
        filtered_routes.append(r)

    spec = get_openapi(
        title="Tiled",
        version=getattr(app, "version", ""),
        description=(
            "Tiled structured data access API — agent-optimised subset. "
            "Exposes read-only data access and authentication endpoints only. "
            "Authenticate by passing an API key in the Authorization header as "
            "'Apikey <SECRET>', via the tiled_api_key cookie, or obtain a token "
            "via the OIDC/device-code auth endpoints below."
        ),
        routes=filtered_routes,
    )

    # Post-process: add enrichments and remove per-operation security noise
    for path_item in spec.get("paths", {}).values():
        for method_op in path_item.values():
            if not isinstance(method_op, dict):
                continue
            oid = method_op.get("operationId", "")
            enrichment = OPERATION_ENRICHMENTS.get(oid, {})
            if enrichment.get("description"):
                method_op["description"] = enrichment["description"]
            if enrichment.get("x-usage-example"):
                method_op["x-usage-example"] = enrichment["x-usage-example"]
            # Strip per-operation security — agents inject auth at the HTTP layer
            method_op.pop("security", None)

    # Signal to agent frameworks that auth is managed externally
    spec["auth"] = []

    return spec
