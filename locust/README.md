# Tiled Load Testing with Locust

Simple load testing for Tiled using the `reader.py` file.

## Quick Start

```bash
# Install dependencies (dev environment includes locust)
pixi install -e dev
```

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

## Headless Mode
Run without the web interface:
```bash
pixi run -e dev locust -f reader.py --headless -u 100 -r 10 -t 60s
```
- `-u 100`: 100 concurrent users
- `-r 10`: Spawn 10 users per second
- `-t 60s`: Run for 60 seconds
