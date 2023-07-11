# Run Tiled Server in a Container

There is an official Tiled container image for use with
[Docker](https://www.docker.com/) or [podman](https://podman.io/).

Download the Tiled container image.

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

**The data and database are inside the container and will not persist outside
it.** Read on to persist it.

## Example: A persistent writable catalog

We will create and mount a local directory, `./storage` which will be used to
hold uploaded data and # a (SQLite) database to index the metadata.

```
mkdir storage/

docker run \
  -p 8000:8000 \
  -e TILED_SINGLE_USER_API_KEY=secret \
  -v ./storage:/storage ghcr.io/bluesky/tiled:latest
```

## Example: Serve a directory of existing files

We will point Tiled at a (read-only) directory of files and ask it to crawl and
serve them. If you don't have scientific data files at hand to try this with, you can
quickly generate some with:

```
# Optional: Generate sample files... TIFF, Excel, HDF5, etc.
python -m tiled.examples.generate_files files/
```

```
docker run \
  -p 8000:8000 \
  -e TILED_SINGLE_USER_API_KEY=secret \
  -v ./files:/files:ro ghcr.io/bluesky/tiled:latest \
  tiled serve directory --host 0.0.0.0 /files
```

## Example: Custom configuration

There are configuration examples locate in the directory `example_configs`
under the Tiled repository root. The container image has one in particular,
`single_user_single_catalog.yml`, copied into the container under
`/deploy/config/`. Override it by mounting a local directory an
`/deploy/config` as shown:

```
docker run \
  -p 8000:8000 \
  -e TILED_SINGLE_USER_API_KEY=secret \
  -v ./config:/deploy/config:ro
```

You may need to mount additional volumes as well.

## Example: Run Tiled with a dashboard of metrics

See {doc}`../how-to/metrics`.

## Next Steps

See {doc}`../explanations/security` and {doc}`../explanations/access-control`
for examples addressing authentication and authorization.

See {doc}`../reference/service-configuration` for a comprehensive reference.
