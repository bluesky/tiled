# Tiled Load Testing with Locust

Load testing for Tiled using Locust. Two test files are available:
- `reader.py` - Tests HTTP read operations and search endpoints
- `streaming.py` - Tests streaming data writes and WebSocket delivery latency

## Quick Start

```bash
# Install dependencies (dev environment includes locust)
pixi install -e dev
```

## Starting Test Server

Before running locust tests, start a Tiled server:

```bash
# Basic server (works for most tests)
uv run tiled serve catalog \
  --host 0.0.0.0 \
  --port 8000 \
  --api-key secret \
  --temp \
  --init
```

For streaming tests with Redis cache (optional):
```bash
# Start Redis first
redis-server

# Start Tiled server with Redis cache
uv run tiled serve catalog \
  --host 0.0.0.0 \
  --port 8000 \
  --api-key secret \
  --cache-uri "redis://localhost:6379" \
  --cache-ttl 60 \
  --temp \
  --init
```

This creates a temporary catalog with:
- API key authentication (key: "secret")
- Temporary writable storage (automatically cleaned up)
- Optional Redis cache for enhanced streaming performance
- Server running on http://localhost:8000

## Reading Performance Tests (`reader.py`)

Tests various HTTP endpoints for reading data, metadata, and search operations.

### Examples
Run with default localhost server (uses default API key 'secret'):
```bash
pixi run -e dev locust -f reader.py --host http://localhost:8000
```

Run with custom API key:
```bash
pixi run -e dev locust -f reader.py --host http://localhost:8000 --api-key your-api-key
```

Run with custom container name (defaults to locust_testing):
```bash
pixi run -e dev locust -f reader.py --host http://localhost:8000 --container-name my_test_container
```

## Streaming Performance Tests (`streaming.py`)

Tests streaming data writes and WebSocket delivery with end-to-end latency measurement.

### Examples
Run with default settings:
```bash
pixi run -e dev locust -f streaming.py --host http://localhost:8000
```

Run with custom API key and node name:
```bash
pixi run -e dev locust -f streaming.py --host http://localhost:8000 --api-key your-api-key --node-name my_test_stream
```

Control user types with environment variables:
```bash
# 2 writers for every 1 streaming reader
WRITER_WEIGHT=2 STREAMING_WEIGHT=1 pixi run -e dev locust -f streaming.py --host http://localhost:8000
```

### Streaming Test Components
- **WriterUser**: Writes timestamped array data to streaming nodes
- **StreamingUser**: Connects via WebSocket to measure write-to-delivery latency

## Headless Mode
Run without the web interface:
```bash
# Reading tests
pixi run -e dev locust -f reader.py --headless -u 100 -r 10 -t 60s --host http://localhost:8000

# Streaming tests
pixi run -e dev locust -f streaming.py --headless -u 10 -r 2 -t 120s --host http://localhost:8000
```
- `-u N`: N concurrent users
- `-r N`: Spawn N users per second
- `-t Ns`: Run for N seconds
