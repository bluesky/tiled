# Tiled Load Testing with Locust

Simple load testing for Tiled using the `test_user.py` file.

## Quick Start

```bash
# Install dependencies (dev environment includes locust)
pixi install -e dev

# Run locust with the test user
pixi run -e dev locust -f test_user.py --host http://localhost:8000
```

This will start the Locust web interface at http://localhost:8089 where you can configure the number of users and spawn rate.

## Configuration

### API Key
Set the API key environment variable (defaults to 'secret'):
```bash
TILED_SINGLE_USER_API_KEY=your-api-key pixi run -e dev locust -f test_user.py --host http://localhost:8000
```

### Headless Mode
Run without the web interface:
```bash
pixi run -e dev locust -f test_user.py --host http://localhost:8000 --headless -u 100 -r 10 -t 60s
```
- `-u 100`: 100 concurrent users
- `-r 10`: Spawn 10 users per second
- `-t 60s`: Run for 60 seconds

### Reduce Logging
```bash
pixi run -e dev locust -f test_user.py --host http://localhost:8000 -L WARNING
```

## What the Test Does

The `TiledUser` class in `test_user.py` simulates users that:
- Wait 1-3 seconds between requests
- Test the root endpoint (`/`)
- Test the metadata endpoint (`/api/v1/metadata`)
- Use Bearer token authentication with the configured API key
