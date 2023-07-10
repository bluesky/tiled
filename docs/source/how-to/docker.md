# Run Tiled using Docker

To run tiled using docker we must first obtain a docker image containing a tiled installation.
To do this run

```
docker pull ghcr.io/bluesky/tiled:latest
```

It is best practice to use a specific tag instead of `latest`.
See the [list of tiled image versions on GitHub](https://github.com/bluesky/tiled/pkgs/container/tiled)
for tags.


## Example: A writable catalog

```
docker run \
  -p 8000:8000 \
  -e TILED_SINGLE_USER_API_KEY=secret \
  ghcr.io/bluesky/tiled:latest
```

**The data and database are inside the container and will be deleted when the
container stops running.** Read on to persist it.

## Example: A persistent writable catalog

```
mkdir storage/
docker run \
  -p 8000:8000 \
  -e TILED_SINGLE_USER_API_KEY=secret \
  -v ./storage:/storage ghcr.io/bluesky/tiled:latest
```

## Example: Serve a directory of existing files

```
docker run \
  -p 8000:8000 \
  -e TILED_SINGLE_USER_API_KEY=secret \
  -v ./files:/files:ro ghcr.io/bluesky/tiled:latest \
  tiled serve directory --host 0.0.0.0 /files
```

## Example: Custom configuration

```
docker run \
  -p 8000:8000 \
  -e TILED_SINGLE_USER_API_KEY=secret \
  -v ./config:/deploy/config:ro
  tiled serve directory --host 0.0.0.0 /files
```

You may need to mount additional volumes as well.

## Example: Run a dashboard with metrics.

See the file `docker-compose.yml` in the Tiled repository root.

With this file the tiled server can be brought up by simply running `docker-compose up`.
