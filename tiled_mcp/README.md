# Tiled MCP Server

An MCP (Model Context Protocol) server that exposes the Tiled API to AI coding agents, enabling AI-assisted data exploration and analysis.

## Installation

The MCP server is included in the `mcp` pixi environment:

```bash
pixi install -e mcp
```

## Configuration

The server is configured via environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `TILED_URL` | Base URL of the Tiled server | `http://localhost:8000` |
| `TILED_API_KEY` | API key for authentication | None (optional) |

## Creating a Read-Only API Key

It's recommended to create a read-only API key:

1. **Create a profile** for the Tiled server:
   ```bash
   tiled profile create https://tiled-demo.nsls2.bnl.gov --name demo
   ```

   For a local server:
   ```bash
   tiled profile create http://localhost:8000 --name local
   ```

2. **Login to the Tiled server:**
   ```bash
   tiled login
   ```

   Or specify a profile if you have multiple:
   ```bash
   tiled login --profile demo
   ```

3. **Create a read-only API key:**
   ```bash
   tiled api_key create --scopes read:metadata --scopes read:data --note "MCP read-only"
   ```

   This outputs the API key secret. Save it securely.

4. **Optional: Add expiration:**
   ```bash
   tiled api_key create --scopes read:metadata --scopes read:data --expires-in 1y --note "MCP read-only"
   ```

### Managing API Keys

```bash
# List your API keys
tiled api_key list

# Revoke an API key (use first 8 characters)
tiled api_key revoke <first-8-chars>
```

## Adding to an MCP Client

### OpenCode

Add the following to your `~/.config/opencode/opencode.json`:

```json
{
  "mcp": {
    "tiled": {
      "type": "local",
      "command": [
        "pixi", "run", "-e", "mcp", "python", "-m", "tiled_mcp"
      ],
      "enabled": true,
      "environment": {
        "TILED_URL": "https://your-tiled-server.example.com",
        "TILED_API_KEY": "<your-api-key>"
      }
    }
  }
}
```

Omit `TILED_API_KEY` for servers with anonymous access.

### Other MCP Clients

The server uses stdio transport. Run it with:

```bash
# Without authentication
pixi run -e mcp python -m tiled_mcp

# With authentication
TILED_URL=http://localhost:8000 TILED_API_KEY=<your-api-key> \
  pixi run -e mcp python -m tiled_mcp
```

## Available Tools

### Discovery & Navigation

| Tool | Description |
|------|-------------|
| `tiled_server_info` | Get server version, capabilities, and supported formats |
| `tiled_search` | Browse and search the data catalog |
| `tiled_metadata` | Get metadata and structure for a specific entry |
| `tiled_distinct` | Get distinct values for filtering |
| `tiled_revisions` | Get revision history for an entry |

### Data Reading

| Tool | Description |
|------|-------------|
| `tiled_array_full` | Read a full array (with optional slicing) |
| `tiled_array_block` | Read a specific chunk of an array |
| `tiled_table_full` | Read a full table/DataFrame |
| `tiled_table_partition` | Read a specific partition of a table |
| `tiled_container_full` | Read contents of a container |
| `tiled_node_full` | Read any node type (deprecated) |
| `tiled_awkward_full` | Read a full Awkward array |
| `tiled_awkward_buffers` | Read Awkward array buffers |

### Assets

| Tool | Description |
|------|-------------|
| `tiled_asset_manifest` | Get manifest for a directory asset |

### Authentication Info

| Tool | Description |
|------|-------------|
| `tiled_whoami` | Get current user information |
| `tiled_apikey_info` | Get current API key details |

### Utility

| Tool | Description |
|------|-------------|
| `tiled_health` | Check server health status |
| `tiled_metrics` | Get Prometheus metrics |

## Example Usage

Once configured, you can ask your AI coding agent questions like:

- "What datasets are available on the Tiled server?"
- "Show me the metadata for the dataset at path/to/data"
- "Read the first 10 rows of the table at experiments/run001"
- "What are the distinct values for the 'sample_type' metadata field?"

## Running Manually (for testing)

```bash
# Test that the server starts
pixi run -e mcp python -m tiled_mcp

# The server uses stdio transport, so it will wait for MCP protocol messages
# Press Ctrl+C to exit
```

## Available Scopes Reference

| Scope | Description |
|-------|-------------|
| `read:metadata` | Read metadata |
| `read:data` | Read data |
| `write:metadata` | Write metadata |
| `write:data` | Write data |
| `create:node` | Add a node |
| `delete:node` | Delete a node |
| `delete:revision` | Delete metadata revisions |
| `register` | Register externally-managed assets |
| `metrics` | Access Prometheus metrics |
