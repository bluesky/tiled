"""
Tiled MCP Server

A simple MCP server that exposes the Tiled API to AI coding agents.
Uses FastMCP with stdio transport.

Configuration via environment variables:
- TILED_URL: Base URL of Tiled server (default: http://localhost:8000)
- TILED_API_KEY: API key for authentication (optional)
"""

import json
import os
from typing import Any

import httpx
from mcp.server.fastmcp import FastMCP

# Configuration from environment
TILED_URL = os.getenv("TILED_URL", "http://localhost:8000")
TILED_API_KEY = os.getenv("TILED_API_KEY")

# HTTP client setup
_headers = {}
if TILED_API_KEY:
    _headers["X-Tiled-Api-Key"] = TILED_API_KEY

client = httpx.Client(
    base_url=TILED_URL,
    timeout=30,
    headers=_headers,
)

# MCP server
mcp = FastMCP("tiled")


def _format_response(response: httpx.Response) -> str:
    """Format HTTP response as a readable string."""
    if response.status_code >= 400:
        return f"Error {response.status_code}: {response.text}"
    try:
        data = response.json()
        return json.dumps(data, indent=2, default=str)
    except Exception:
        return response.text


def _build_params(**kwargs) -> dict:
    """Build query parameters, filtering out None values."""
    return {k: v for k, v in kwargs.items() if v is not None}


# Keys from BlueskyRun start document worth surfacing in compact summaries
_INTERESTING_START_KEYS = {
    "uid",
    "scan_id",
    "plan_name",
    "plan_type",
    "note",
    "sample",
    "detectors",
    "motors",
    "XEng",
    "num_eng",
    "operator",
    "exposure_time",
    "num_bkg_images",
    "num_dark_images",
    "chunk_size",
    "data_session",
}

_INTERESTING_STOP_KEYS = {"exit_status", "num_events", "time"}


def _compact_structure(structure: dict | None) -> dict | None:
    """Return a compact version of a structure dict -- shape/dims/data_type only."""
    if not structure:
        return None
    compact: dict[str, Any] = {}
    if "shape" in structure:
        compact["shape"] = structure["shape"]
    if "dims" in structure:
        compact["dims"] = structure["dims"]
    if "data_type" in structure:
        dt = structure["data_type"]
        compact["data_type"] = f"{dt.get('kind', '')}{dt.get('itemsize', '')}"
    if "columns" in structure:
        cols = structure["columns"]
        compact["num_columns"] = len(cols)
        # Show first few column names as a preview
        if len(cols) <= 10:
            compact["columns"] = cols
        else:
            compact["columns_preview"] = cols[:8] + ["..."]
    if "count" in structure:
        compact["count"] = structure["count"]
    if "resizable" in structure:
        compact["resizable"] = structure["resizable"]
    return compact


def _compact_metadata(metadata: dict | None) -> dict | None:
    """Extract only the interesting metadata from a BlueskyRun."""
    if not metadata:
        return None
    compact: dict[str, Any] = {}
    # Handle BlueskyRun-style metadata with start/stop docs
    if "start" in metadata:
        start = metadata["start"]
        compact["start"] = {
            k: v for k, v in start.items() if k in _INTERESTING_START_KEYS
        }
    if "stop" in metadata:
        stop = metadata["stop"]
        compact["stop"] = {k: v for k, v in stop.items() if k in _INTERESTING_STOP_KEYS}
    # If it's not a BlueskyRun (no start/stop), return keys summary
    if "start" not in metadata and "stop" not in metadata:
        # For event stream metadata (data_keys), just list the key names
        if any(isinstance(v, dict) and "dtype" in v for v in metadata.values()):
            compact["data_keys"] = list(metadata.keys())
        else:
            # For other metadata, return as-is but truncate if large
            if len(json.dumps(metadata, default=str)) < 2000:
                return metadata
            compact["keys"] = list(metadata.keys())
    return compact


def _compact_entry(entry: dict) -> dict:
    """Build a compact summary of a single catalog entry."""
    attrs = entry.get("attributes", {})
    summary: dict[str, Any] = {"id": entry.get("id")}

    sf = attrs.get("structure_family")
    if sf:
        summary["structure_family"] = sf

    specs = attrs.get("specs")
    if specs:
        summary["specs"] = [s.get("name") for s in specs]

    structure = attrs.get("structure")
    compact_struct = _compact_structure(structure)
    if compact_struct:
        summary["structure"] = compact_struct

    metadata = attrs.get("metadata")
    compact_meta = _compact_metadata(metadata)
    if compact_meta:
        summary["metadata"] = compact_meta

    return summary


def _compact_search_response(data: dict) -> str:
    """Format a search response compactly."""
    entries = data.get("data", [])
    meta = data.get("meta", {})
    result: dict[str, Any] = {
        "count": meta.get("count", len(entries)),
        "entries": [_compact_entry(e) for e in entries],
    }
    return json.dumps(result, indent=2, default=str)


def _compact_metadata_response(data: dict) -> str:
    """Format a metadata response compactly."""
    entry = data.get("data", data)
    return json.dumps(_compact_entry(entry), indent=2, default=str)


# =============================================================================
# Discovery & Navigation Tools
# =============================================================================


@mcp.tool()
def tiled_server_info() -> str:
    """
    Get information about the Tiled server.

    Returns server version, API version, supported formats, available queries,
    and authentication configuration.
    """
    response = client.get("/api/v1/")
    return _format_response(response)


@mcp.tool()
def tiled_search(
    path: str = "",
    page_offset: int = 0,
    page_limit: int = 20,
    sort: str | None = None,
    select_metadata: str | None = None,
    include_data_sources: bool = False,
    verbose: bool = False,
) -> str:
    """
    Search and browse the Tiled data catalog.

    Args:
        path: Path in the catalog to search (empty string for root)
        page_offset: Number of results to skip (for pagination)
        page_limit: Maximum number of results to return (max 100)
        sort: Sort order (e.g., "metadata.time" or "-metadata.time" for descending)
        select_metadata: JMESPath expression to filter metadata fields
        include_data_sources: Include data source information in results
        verbose: If True, return the full raw response. Default False returns a compact summary.

    Returns:
        List of entries at the given path with their metadata and structure info.
    """
    params = _build_params(
        **{
            "page[offset]": page_offset,
            "page[limit]": min(page_limit, 100),
            "sort": sort,
            "select_metadata": select_metadata,
            "include_data_sources": include_data_sources,
        }
    )
    response = client.get(f"/api/v1/search/{path}", params=params)
    if response.status_code >= 400:
        return f"Error {response.status_code}: {response.text}"
    if verbose:
        return _format_response(response)
    return _compact_search_response(response.json())


@mcp.tool()
def tiled_metadata(
    path: str,
    select_metadata: str | None = None,
    include_data_sources: bool = False,
    verbose: bool = False,
) -> str:
    """
    Get metadata and structure information for a specific entry.

    Args:
        path: Path to the entry in the catalog
        select_metadata: JMESPath expression to filter metadata fields
        include_data_sources: Include data source information
        verbose: If True, return the full raw response. Default False returns a compact summary.

    Returns:
        Entry metadata, structure, and links.
    """
    params = _build_params(
        select_metadata=select_metadata,
        include_data_sources=include_data_sources,
    )
    response = client.get(f"/api/v1/metadata/{path}", params=params)
    if response.status_code >= 400:
        return f"Error {response.status_code}: {response.text}"
    if verbose:
        return _format_response(response)
    return _compact_metadata_response(response.json())


@mcp.tool()
def tiled_distinct(
    path: str = "",
    metadata: list[str] | None = None,
    structure_families: bool = False,
    specs: bool = False,
    counts: bool = False,
) -> str:
    """
    Get distinct values for metadata fields, useful for filtering.

    Args:
        path: Path in the catalog
        metadata: List of metadata fields to get distinct values for
        structure_families: Include distinct structure families
        specs: Include distinct specs
        counts: Include counts for each distinct value

    Returns:
        Distinct values for the requested fields.
    """
    params: dict[str, Any] = {
        "structure_families": structure_families,
        "specs": specs,
        "counts": counts,
    }
    if metadata:
        params["metadata"] = metadata
    response = client.get(f"/api/v1/distinct/{path}", params=params)
    return _format_response(response)


@mcp.tool()
def tiled_revisions(
    path: str,
    page_offset: int = 0,
    page_limit: int = 20,
) -> str:
    """
    Get revision history for an entry.

    Args:
        path: Path to the entry
        page_offset: Number of results to skip (for pagination)
        page_limit: Maximum number of results to return

    Returns:
        List of revisions with timestamps and changes.
    """
    params = _build_params(
        **{
            "page[offset]": page_offset,
            "page[limit]": page_limit,
        }
    )
    response = client.get(f"/api/v1/revisions/{path}", params=params)
    return _format_response(response)


# =============================================================================
# Data Reading Tools
# =============================================================================


@mcp.tool()
def tiled_array_full(
    path: str,
    slice: str | None = None,
    format: str | None = None,
) -> str:
    """
    Read a full array (or a slice of it).

    Args:
        path: Path to the array in the catalog
        slice: NumPy-style slice string (e.g., "0:10", "0:10,0:5", ":,0")
        format: Output format (e.g., "json", "csv"). Default returns JSON.

    Returns:
        Array data in the requested format.
    """
    params = _build_params(slice=slice, format=format or "json")
    response = client.get(f"/api/v1/array/full/{path}", params=params)
    return _format_response(response)


@mcp.tool()
def tiled_array_block(
    path: str,
    block: str,
    slice: str | None = None,
    format: str | None = None,
) -> str:
    """
    Read a specific block/chunk of an array.

    Args:
        path: Path to the array in the catalog
        block: Block indices as comma-separated values (e.g., "0,0" for first block of 2D array)
        slice: Additional slice within the block
        format: Output format (e.g., "json", "csv")

    Returns:
        Block data in the requested format.
    """
    params = _build_params(block=block, slice=slice, format=format or "json")
    response = client.get(f"/api/v1/array/block/{path}", params=params)
    return _format_response(response)


@mcp.tool()
def tiled_table_full(
    path: str,
    column: list[str] | None = None,
    format: str | None = None,
) -> str:
    """
    Read a full table (DataFrame).

    Args:
        path: Path to the table in the catalog
        column: List of column names to include (None for all columns)
        format: Output format (e.g., "json", "csv", "parquet")

    Returns:
        Table data in the requested format.
    """
    params: dict[str, Any] = {"format": format or "json"}
    if column:
        params["column"] = column
    response = client.get(f"/api/v1/table/full/{path}", params=params)
    return _format_response(response)


@mcp.tool()
def tiled_table_partition(
    path: str,
    partition: int,
    column: list[str] | None = None,
    format: str | None = None,
) -> str:
    """
    Read a specific partition of a table.

    Args:
        path: Path to the table in the catalog
        partition: Partition index (0-based)
        column: List of column names to include
        format: Output format

    Returns:
        Partition data in the requested format.
    """
    params: dict[str, Any] = {"partition": partition, "format": format or "json"}
    if column:
        params["column"] = column
    response = client.get(f"/api/v1/table/partition/{path}", params=params)
    return _format_response(response)


@mcp.tool()
def tiled_container_full(
    path: str,
    field: list[str] | None = None,
    format: str | None = None,
) -> str:
    """
    Read contents of a container (group of datasets).

    Args:
        path: Path to the container in the catalog
        field: List of field names to include
        format: Output format

    Returns:
        Container contents in the requested format.
    """
    params: dict[str, Any] = {"format": format or "json"}
    if field:
        params["field"] = field
    response = client.get(f"/api/v1/container/full/{path}", params=params)
    return _format_response(response)


@mcp.tool()
def tiled_node_full(
    path: str,
    field: list[str] | None = None,
    format: str | None = None,
) -> str:
    """
    Read any node type (container or table). Deprecated in favor of specific endpoints.

    Args:
        path: Path to the node in the catalog
        field: List of field names to include
        format: Output format

    Returns:
        Node data in the requested format.
    """
    params: dict[str, Any] = {"format": format or "json"}
    if field:
        params["field"] = field
    response = client.get(f"/api/v1/node/full/{path}", params=params)
    return _format_response(response)


@mcp.tool()
def tiled_awkward_full(
    path: str,
    format: str | None = None,
) -> str:
    """
    Read a full Awkward array.

    Args:
        path: Path to the awkward array in the catalog
        format: Output format

    Returns:
        Awkward array data.
    """
    params = _build_params(format=format or "json")
    response = client.get(f"/api/v1/awkward/full/{path}", params=params)
    return _format_response(response)


@mcp.tool()
def tiled_awkward_buffers(
    path: str,
    form_key: list[str] | None = None,
    format: str | None = None,
) -> str:
    """
    Read Awkward array buffers.

    Args:
        path: Path to the awkward array in the catalog
        form_key: List of form keys to include
        format: Output format

    Returns:
        Awkward array buffer data.
    """
    params: dict[str, Any] = {"format": format or "json"}
    if form_key:
        params["form_key"] = form_key
    response = client.get(f"/api/v1/awkward/buffers/{path}", params=params)
    return _format_response(response)


# =============================================================================
# Asset Tools
# =============================================================================


@mcp.tool()
def tiled_asset_manifest(
    path: str,
    id: int,
) -> str:
    """
    Get the manifest (list of files) for a directory asset.

    Args:
        path: Path to the entry in the catalog
        id: Asset ID

    Returns:
        List of files in the asset directory.
    """
    params = {"id": id}
    response = client.get(f"/api/v1/asset/manifest/{path}", params=params)
    return _format_response(response)


# =============================================================================
# Auth Info Tools (read-only)
# =============================================================================


@mcp.tool()
def tiled_whoami() -> str:
    """
    Get information about the currently authenticated user.

    Returns:
        Current user's identity, roles, and permissions.
    """
    response = client.get("/api/v1/auth/whoami")
    return _format_response(response)


@mcp.tool()
def tiled_apikey_info() -> str:
    """
    Get information about the current API key.

    Returns:
        API key details including scopes and expiration.
    """
    response = client.get("/api/v1/auth/apikey")
    return _format_response(response)


# =============================================================================
# Utility Tools
# =============================================================================


@mcp.tool()
def tiled_health() -> str:
    """
    Check if the Tiled server is healthy and ready.

    Returns:
        Health status of the server.
    """
    response = client.get("/healthz")
    return _format_response(response)


@mcp.tool()
def tiled_metrics() -> str:
    """
    Get Prometheus metrics from the server.

    Returns:
        Server metrics in Prometheus format.
    """
    response = client.get("/api/v1/metrics")
    if response.status_code >= 400:
        return f"Error {response.status_code}: {response.text}"
    return response.text


def main():
    """Run the MCP server with stdio transport."""
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
