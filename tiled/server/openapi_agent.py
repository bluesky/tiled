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
- Dereferencing all ``$ref`` pointers so the spec is fully self-contained
  (many agent frameworks cannot resolve JSON ``$ref`` pointers)
"""

import copy

from fastapi.openapi.utils import get_openapi
from fastapi.routing import APIRoute

# ---------------------------------------------------------------------------
# Short, verb-noun operationId overrides keyed by (path, method)
# ---------------------------------------------------------------------------
OPERATION_ID_MAP: dict[tuple[str, str], str] = {
    ("/healthz", "GET"): "CheckHealth",
    ("/api/v1/", "GET"): "GetServerInfo",
    ("/api/v1/search/{path}", "GET"): "SearchNodes",
    ("/api/v1/metadata/{path}", "GET"): "GetMetadata",
    ("/api/v1/array/full/{path}", "GET"): "GetArrayFull",
    ("/api/v1/table/full/{path}", "GET"): "GetTableFull",
    ("/api/v1/awkward/full/{path}", "GET"): "GetAwkwardFull",
    ("/api/v1/asset/manifest/{path}", "GET"): "GetAssetManifest",
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
    "GetMetadata": {
        "x-usage-example": (
            "GET /api/v1/metadata/experiment/run_001/detector  "
            "- get metadata for a specific node"
        ),
    },
    "GetArrayFull": {
        "x-usage-example": (
            "GET /api/v1/array/full/path/to/array?format=application/json  "
            "- fetch entire array\n"
            "GET /api/v1/array/full/path/to/array?slice=0:10&format=application/json  "
            "- fetch first 10 elements"
        ),
    },
    "GetTableFull": {
        "x-usage-example": (
            "GET /api/v1/table/full/path/to/table?format=application/json  "
            "- fetch entire table as JSON"
        ),
    },
    "GetAwkwardFull": {
        "x-usage-example": (
            "GET /api/v1/awkward/full/path/to/node?format=application/json"
        ),
    },
    "GetAssetManifest": {
        "description": (
            "List the assets (files) associated with a node, including their IDs, "
            "paths, and sizes."
        ),
        "x-usage-example": "GET /api/v1/asset/manifest/path/to/node?id=1",
    },
}


# ---------------------------------------------------------------------------
# $ref dereferencing
# ---------------------------------------------------------------------------
# Maximum recursion depth for inlining $ref pointers.  Schemas that reference
# themselves (directly or transitively) are inlined up to this depth, then
# replaced with a permissive ``{"type": "object"}`` stub to break the cycle.
_MAX_REF_DEPTH = 3


def _resolve_ref(ref_string: str, root: dict) -> dict:
    """Follow a JSON pointer like ``#/components/schemas/Foo``."""
    parts = ref_string.lstrip("#/").split("/")
    node = root
    for part in parts:
        node = node[part]
    return node


def _inline_refs(node, root: dict, seen: frozenset[str] = frozenset(), depth: int = 0):
    """
    Recursively replace every ``{"$ref": "..."}`` with the resolved content.

    Parameters
    ----------
    node
        Current position in the schema tree.
    root
        The full original schema (needed to look up ``$ref`` targets).
    seen
        Set of ``$ref`` targets on the *current* recursion path, used to
        detect cycles.
    depth
        Current inlining depth.

    Returns
    -------
    The same structure with all ``$ref`` pointers replaced inline.
    """
    if isinstance(node, list):
        return [_inline_refs(item, root, seen, depth) for item in node]

    if not isinstance(node, dict):
        return node

    if "$ref" in node and isinstance(node["$ref"], str):
        ref = node["$ref"]
        if ref in seen or depth >= _MAX_REF_DEPTH:
            # Cycle or too deep — use a permissive stub.
            return {"type": "object", "description": f"(see {ref.split('/')[-1]})"}
        resolved = copy.deepcopy(_resolve_ref(ref, root))
        return _inline_refs(resolved, root, seen | {ref}, depth + 1)

    return {k: _inline_refs(v, root, seen, depth) for k, v in node.items()}


def _deref_openapi(spec: dict) -> dict:
    """
    Dereference all ``$ref`` pointers in *spec* and drop ``components``.

    Returns a new dict; *spec* is not mutated.
    """
    dereferenced = _inline_refs(spec, root=spec)
    dereferenced.pop("components", None)
    return dereferenced


def _should_include(route: APIRoute) -> bool:
    """Return True if this route belongs in the agent spec."""
    methods = route.methods or set()
    path = route.path_format
    if "GET" in methods:
        return path.startswith("/api/v1") or path == "/healthz"
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
        # Only keep the paths decalred in OPERATION_ID_MAP to keep the spec minimal
        # and focused on key data access endpoints.
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
            "Exposes read-only data access only."
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

    # Dereference all $ref pointers so the spec is fully self-contained.
    # Many agent frameworks (OpenAI function calling, some LangChain/CrewAI
    # integrations) cannot resolve JSON $ref pointers and need everything
    # inlined.
    spec = _deref_openapi(spec)

    return spec
