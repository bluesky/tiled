# Tiled Load Testing with Locust

Simple load testing for Tiled using the `reader.py` file.

## Quick Start

```bash
# Install dependencies (dev environment includes locust)
pixi install -e dev

# Run locust with the test user
pixi run -e dev locust -f reader.py --host http://localhost:8000
```

This will start the Locust web interface at http://localhost:8089 where you can configure the number of users and spawn rate.

## With an API key.

### API Key
Set the API key environment variable (defaults to 'secret'):
```bash
TILED_SINGLE_USER_API_KEY=your-api-key pixi run -e dev locust -f reader.py --host http://localhost:8000
```

### Headless Mode
Run without the web interface:
```bash
pixi run -e dev locust -f reader.py --host http://localhost:8000 --headless -u 100 -r 10 -t 60s
```
- `-u 100`: 100 concurrent users
- `-r 10`: Spawn 10 users per second
- `-t 60s`: Run for 60 seconds
