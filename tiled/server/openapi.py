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
- Removing schema noise that wastes agent context tokens (422 responses,
  ``title`` fields, internal-only parameters, etc.)
"""

import copy

from fastapi.openapi.utils import get_openapi
from fastapi.routing import APIRoute

# List of (path, method) pairs for which to set clean operationIds. Only these
# key endpoints are included in the agent spec to keep it minimal and focused.
# Short, verb-noun operationId overrides keyed by (path, method)
OPERATION_ID_MAP: dict[tuple[str, str], str] = {
    ("/healthz", "GET"): "CheckHealth",
    ("/api/v1/search/{path}", "GET"): "SearchNodes",
    ("/api/v1/metadata/{path}", "GET"): "GetMetadata",
    ("/api/v1/array/full/{path}", "GET"): "GetArrayFull",
    ("/api/v1/table/full/{path}", "GET"): "GetTableFull",
    ("/api/v1/awkward/full/{path}", "GET"): "GetAwkwardFull",
    ("/api/v1/asset/manifest/{path}", "GET"): "GetAssetManifest",
}

# Agent-only enrichments for key endpoints.
# ``x-usage-example`` is an OpenAPI extension field that helps agents
# understand how to call each endpoint with concrete examples.
# Descriptions now live at the endpoint definitions in router.py / app.py
# so they appear in both the default and agent schemas.
OPERATION_ENRICHMENTS: dict[str, dict] = {
    "CheckHealth": {
        "x-usage-example": "GET /healthz",
    },
    "SearchNodes": {
        "x-usage-example": (
            "GET /api/v1/search//  "
            "- list root entries (use '/' as path for root)\n"
            "GET /api/v1/search/experiment/run_001  "
            "- list children of experiment/run_001\n"
            "GET /api/v1/search//?page[limit]=5  "
            "- first 5 results only\n"
            "GET /api/v1/search//?filter[eq][condition][key]=sample"
            "&filter[eq][condition][value]=gold  "
            "- entries where metadata.sample == 'gold'\n"
            "GET /api/v1/search//?filter[fulltext][condition][text]=temperature  "
            "- full-text search\n"
            "GET /api/v1/search//?filter[structure_family][condition][value]=table  "
            "- only table nodes"
        ),
    },
    "GetMetadata": {
        "x-usage-example": (
            "GET /api/v1/metadata/experiment/run_001  "
            "- full metadata for a node\n"
            "GET /api/v1/metadata/experiment/run_001"
            "?select_metadata=sample_name  "
            "- only the sample_name field"
        ),
    },
    "GetArrayFull": {
        "x-usage-example": (
            "GET /api/v1/array/full/path/to/array?format=application/json  "
            "- full array as JSON\n"
            "GET /api/v1/array/full/path/to/array"
            "?slice=0:10&format=application/json  "
            "- first 10 elements"
        ),
    },
    "GetTableFull": {
        "x-usage-example": (
            "GET /api/v1/table/full/path/to/table?format=application/json  "
            "- full table as JSON\n"
            "GET /api/v1/table/full/path/to/table"
            "?column=time&column=value&format=application/json  "
            "- only time and value columns"
        ),
    },
    "GetAwkwardFull": {
        "x-usage-example": (
            "GET /api/v1/awkward/full/path/to/node?format=application/json"
        ),
    },
    "GetAssetManifest": {
        "x-usage-example": "GET /api/v1/asset/manifest/path/to/node?id=1",
    },
}


# $ref dereferencing
# Maximum recursion depth for inlining $ref pointers.  Schemas that reference
# themselves (directly or transitively) are inlined up to this depth, then
# replaced with a permissive ``{"type": "object"}`` stub to break the cycle.
_MAX_REF_DEPTH = 16


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


# Schema simplification — strip noise that wastes agent context tokens
# Parameters that are only used by internal clients (Tiled Python client,
# programmatic downloaders) and have no value for an LLM agent.
_PARAMS_TO_STRIP = {
    # Internal client parameters
    "filename",
    "expected_shape",
    "root_path",
    # Search/metadata display params agents don't need
    "max_depth",
    "omit_links",
    "include_data_sources",
    # fields — the default returns all useful fields; exposing the array param
    # confuses agents into using wrong syntax (e.g. fields=['metadata','structure'])
    "fields",
    # Rarely useful filter types — agents can use eq/contains/fulltext instead
    "filter[lookup][condition][key]",
    "filter[keys_filter][condition][keys]",
    "filter[regex][condition][key]",
    "filter[regex][condition][pattern]",
    "filter[regex][condition][case_sensitive]",
    "filter[noteq][condition][key]",
    "filter[noteq][condition][value]",
    "filter[notin][condition][key]",
    "filter[notin][condition][value]",
    "filter[keypresent][condition][key]",
    "filter[keypresent][condition][exists]",
    "filter[like][condition][key]",
    "filter[like][condition][pattern]",
    # Internal access-control filter
    "filter[access_blob_filter][condition][user_id]",
    "filter[access_blob_filter][condition][tags]",
    # Specs filter — not useful for agents
    "filter[specs][condition][include]",
    "filter[specs][condition][exclude]",
}

# Attribute properties to strip from NodeAttributes in response schemas.
# These are internal implementation details that waste agent context tokens.
_ATTRS_TO_STRIP = {
    "access_blob",
    "sorting",
    "data_sources",
    "ancestors",
    "specs",
    "structure_family",
}

# Agent-friendly descriptions for filter parameters.
_PARAM_DESCRIPTIONS: dict[str, str] = {
    "path": (
        "Catalog path. Use '/' for root. "
        "For nested nodes use e.g. 'experiment/run_001' (no leading slash)."
    ),
    "select_metadata": (
        "JMESPath expression to extract specific metadata fields. "
        "Example: 'sample_name' or '{name: sample_name, temp: temperature}'."
    ),
    "page[offset]": "Number of results to skip (for pagination). Default 0.",
    "page[limit]": "Maximum number of results to return (max 300). Default 100.",
    "sort": (
        "Sort results by a metadata field. Prefix with '-' for descending. "
        "Example: 'metadata.time' or '-metadata.time'."
    ),
    "filter[fulltext][condition][text]": (
        "Full-text search across all metadata values. "
        "Example: 'temperature' finds entries with that word in any metadata field."
    ),
    "filter[eq][condition][key]": "Metadata field name for equality filter.",
    "filter[eq][condition][value]": "Value to match for equality filter.",
    "filter[comparison][condition][operator]": (
        "Comparison operator: lt (less than), gt (greater than), le, ge."
    ),
    "filter[comparison][condition][key]": "Metadata field name for comparison filter.",
    "filter[comparison][condition][value]": "Value to compare against.",
    "filter[contains][condition][key]": (
        "Metadata field name (must be a list/array) for contains filter."
    ),
    "filter[contains][condition][value]": "Value that must be contained in the list.",
    "filter[in][condition][key]": "Metadata field name for in-list filter.",
    "filter[in][condition][value]": "List of acceptable values.",
    "filter[structure_family][condition][value]": (
        "Filter by structure type: array, table, container, awkward, or sparse."
    ),
    "format": (
        "Output format media type. Use 'application/json' for JSON. "
        "Other options depend on the data type (e.g. 'text/csv' for tables)."
    ),
    "slice": (
        "NumPy-style slice to select a subset of the array. "
        "Examples: '0:10' (first 10 elements), '0:5,0:3' (2D slice), ':,0' (first column)."
    ),
    "column": "List of column names to include (omit for all columns).",
    "id": "Asset ID (integer) for the manifest to retrieve.",
}


def _strip_titles(node):
    """Recursively remove ``title`` keys from schema dicts.

    These are Swagger UI decoration that contribute nothing to an agent's
    understanding of the API while consuming context tokens.
    """
    if isinstance(node, list):
        return [_strip_titles(item) for item in node]
    if not isinstance(node, dict):
        return node
    return {k: _strip_titles(v) for k, v in node.items() if k != "title"}


def _simplify_nullable(node):
    """Collapse ``anyOf: [{schema}, {type: null}]`` into just the schema.

    FastAPI/Pydantic generates ``anyOf`` wrappers for every ``Optional`` field,
    which doubles the schema size.  Agents don't need to distinguish null from
    absent — they just need to know the shape of the non-null value.
    """
    if isinstance(node, list):
        return [_simplify_nullable(item) for item in node]
    if not isinstance(node, dict):
        return node

    # Process children first (bottom-up)
    result = {k: _simplify_nullable(v) for k, v in node.items()}

    # Collapse anyOf: [{real_schema}, {type: null}] → real_schema
    if "anyOf" in result and isinstance(result["anyOf"], list):
        non_null = [b for b in result["anyOf"] if b != {"type": "null"}]
        if len(non_null) == 1 and len(result["anyOf"]) == 2:
            # Merge non-null branch into current dict, dropping the anyOf key
            collapsed = dict(non_null[0])
            # Preserve any sibling keys (like description) from the parent
            for k, v in result.items():
                if k != "anyOf":
                    collapsed.setdefault(k, v)
            return collapsed

    return result


def _simplify_structure_schema(schema: dict) -> dict:
    """Replace the deeply nested ``structure`` schema with a compact version.

    The full structure schema includes detailed dtype definitions, chunk
    layouts, and macro/micro structures that can be 8+ KB.  For an agent,
    a simple ``{type: object}`` with a description is sufficient — the agent
    can inspect actual structure values in API responses.
    """
    return {
        "type": "object",
        "description": (
            "Shape and type information for this node. "
            "For arrays: includes shape, data_type (dtype), and chunks. "
            "For tables: includes column names and types. "
            "For containers: includes count of children. "
            "Inspect actual values via the API for details."
        ),
    }


# Properties to strip from Resource objects (the per-entry wrapper).
# ``links`` and ``meta`` are navigation aids the agent can derive from
# the node path and structure_family.
_RESOURCE_PROPS_TO_STRIP = {"links", "meta"}

# JSON Schema validation keywords that waste agent tokens.
# These help request-side validation but add no value for an agent
# that just needs to understand the shape of the data.
_VALIDATION_KEYS_TO_STRIP = {
    "maxLength",
    "minLength",
    "maxItems",
    "minItems",
    "minimum",
    "maximum",
    "pattern",
    "additionalProperties",
}


def _simplify_node_attributes(attrs_schema: dict) -> dict:
    """Simplify the NodeAttributes schema for agent consumption.

    - Remove internal properties (access_blob, sorting, data_sources, ancestors)
    - Replace the deeply nested ``structure`` with a compact summary
    - Clean up the ``required`` list
    """
    props = attrs_schema.get("properties", {})

    # Remove internal properties
    for key in _ATTRS_TO_STRIP:
        props.pop(key, None)

    # Replace ``structure`` with compact version
    if "structure" in props:
        props["structure"] = _simplify_structure_schema(props["structure"])

    # Clean up required list — remove entirely if empty
    if "required" in attrs_schema:
        cleaned = [r for r in attrs_schema["required"] if r not in _ATTRS_TO_STRIP]
        if cleaned:
            attrs_schema["required"] = cleaned
        else:
            del attrs_schema["required"]

    return attrs_schema


def _simplify_resource(schema: dict) -> dict:
    """Simplify a Resource object (the per-entry wrapper around NodeAttributes).

    - Strip ``links`` and ``meta`` properties
    - Collapse ``id`` anyOf to plain ``{type: string}``
    - Update ``required`` list
    """
    props = schema.get("properties", {})

    # Strip links/meta
    for key in _RESOURCE_PROPS_TO_STRIP:
        props.pop(key, None)

    # Collapse id: anyOf[{type: string}, {type: string, format: uuid}] → {type: string}
    id_schema = props.get("id", {})
    if "anyOf" in id_schema:
        props["id"] = {"type": "string"}

    # Clean required list — remove entirely if empty
    if "required" in schema:
        cleaned = [r for r in schema["required"] if r not in _RESOURCE_PROPS_TO_STRIP]
        if cleaned:
            schema["required"] = cleaned
        else:
            del schema["required"]

    return schema


def _strip_validation_keywords(node):
    """Recursively remove JSON Schema validation keywords that waste tokens."""
    if isinstance(node, list):
        return [_strip_validation_keywords(item) for item in node]
    if not isinstance(node, dict):
        return node
    return {
        k: _strip_validation_keywords(v)
        for k, v in node.items()
        if k not in _VALIDATION_KEYS_TO_STRIP
    }


def _is_resource_schema(schema: dict) -> bool:
    """Detect a Resource object by its characteristic shape: {id, attributes}."""
    props = schema.get("properties", {})
    return "id" in props and "attributes" in props


def _simplify_response_schema(schema: dict) -> dict:
    """Walk the response schema and simplify Resource/NodeAttributes."""
    if isinstance(schema, list):
        return [_simplify_response_schema(item) for item in schema]
    if not isinstance(schema, dict):
        return schema

    # Recurse into all values first
    result = {k: _simplify_response_schema(v) for k, v in schema.items()}

    # Detect NodeAttributes by checking for characteristic properties
    props = result.get("properties", {})
    if (
        "structure_family" in props
        and "metadata" in props
        and ("structure" in props or "specs" in props)
    ):
        result = _simplify_node_attributes(result)

    # Detect Resource wrapper and simplify it
    if _is_resource_schema(result):
        result = _simplify_resource(result)

    return result


def _add_param_descriptions(params: list[dict]) -> list[dict]:
    """Add agent-friendly descriptions to parameters that lack them."""
    for p in params:
        name = p.get("name", "")
        if name in _PARAM_DESCRIPTIONS and not p.get("description"):
            p["description"] = _PARAM_DESCRIPTIONS[name]
    return params


def _collapse_array_params(params: list[dict]) -> list[dict]:
    """Collapse ``{type: array, items: <schema>}`` query params to just ``<schema>``.

    FastAPI types multi-value query params (like filters) as arrays, but agents
    almost always pass a single value.  The ``array`` wrapper confuses LLMs
    into generating ``?key=['a','b']`` instead of ``?key=a&key=b``.
    Replacing with the scalar items schema avoids this.
    """
    for p in params:
        if p.get("in") != "query":
            continue
        schema = p.get("schema", {})
        if schema.get("type") == "array" and "items" in schema:
            p["schema"] = schema["items"] if schema["items"] else {"type": "string"}
    return params


def _simplify_spec(spec: dict) -> dict:
    """Apply agent-oriented simplifications to a dereferenced spec.

    Mutations are made in-place for efficiency (called on an already-copied
    spec from ``_deref_openapi``).

    Simplifications applied:
    - Remove 422 Validation Error responses (agents don't need the error schema)
    - Unwrap response envelope: replace ``{data, error, links, meta}`` wrapper
      with just the contents of ``data``, since agents only need the payload
    - Simplify Resource objects (strip links/meta, collapse id anyOf)
    - Simplify NodeAttributes (strip internal fields, compact structure schema)
    - Strip JSON Schema validation keywords (maxLength, maxItems, etc.)
    - Collapse ``anyOf: [X, null]`` nullable wrappers
    - Remove ``title`` fields throughout (Swagger UI decoration)
    - Remove internal-only parameters and agent-irrelevant filters
    - Collapse array-typed query params to scalar item schemas
    - Add agent-friendly parameter descriptions
    """
    for path_item in spec.get("paths", {}).values():
        for method_op in path_item.values():
            if not isinstance(method_op, dict):
                continue

            # Drop 422 responses
            responses = method_op.get("responses", {})
            responses.pop("422", None)

            # Unwrap the response envelope.  Tiled wraps every 200 response in
            # {"data": <payload>, "error": ..., "links": ..., "meta": ...}.
            # Replace the schema with just the ``data`` property's schema so
            # agents see the actual payload shape directly.
            resp_200 = responses.get("200", {})
            schema = (
                resp_200.get("content", {})
                .get("application/json", {})
                .get("schema", {})
            )
            data_schema = schema.get("properties", {}).get("data")
            if data_schema is not None:
                resp_200.setdefault("content", {}).setdefault("application/json", {})[
                    "schema"
                ] = data_schema

            # Drop internal-only parameters
            params = method_op.get("parameters")
            if params:
                params = [p for p in params if p.get("name") not in _PARAMS_TO_STRIP]
                params = _add_param_descriptions(params)
                method_op["parameters"] = params

    # Simplify response schemas (Resource, NodeAttributes, structure, etc.)
    if "paths" in spec:
        spec["paths"] = _simplify_response_schema(spec["paths"])

    # Strip JSON Schema validation keywords (maxLength, maxItems, etc.)
    spec = _strip_validation_keywords(spec)

    # Collapse nullable anyOf wrappers
    spec = _simplify_nullable(spec)

    # Strip title fields from paths (schema decoration) but not from info
    if "paths" in spec:
        spec["paths"] = _strip_titles(spec["paths"])

    # Collapse array-typed query params to scalar schemas.  This must run
    # AFTER _simplify_nullable (which unwraps anyOf[array, null] → array).
    for path_item in spec.get("paths", {}).values():
        for method_op in path_item.values():
            if not isinstance(method_op, dict):
                continue
            params = method_op.get("parameters")
            if params:
                method_op["parameters"] = _collapse_array_params(params)

    return spec


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
            "Tiled is a structured data access server. Data is organized as a "
            "tree of nodes. Each node is one of: container (has children), "
            "array (N-dimensional numeric data), table (columnar data), or "
            "awkward (variable-length nested data). Use SearchNodes to browse "
            "and filter the tree, GetMetadata to inspect a node (different nodes "
            "may have different metadata structure), and "
            "GetArrayFull/GetTableFull to read actual data values. "
            "All endpoints are read-only. Responses are JSON."
        ),
        routes=filtered_routes,
    )

    # Post-process: add agent-only enrichments (x-usage-example) and
    # remove per-operation security noise.  Descriptions are inherited from
    # the endpoint definitions in router.py / app.py.
    for path_item in spec.get("paths", {}).values():
        for method_op in path_item.values():
            if not isinstance(method_op, dict):
                continue
            oid = method_op.get("operationId", "")
            enrichment = OPERATION_ENRICHMENTS.get(oid, {})
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

    # Strip noise that wastes agent context tokens.
    spec = _simplify_spec(spec)

    return spec
