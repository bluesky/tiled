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

```{note}

Some of the examples below set an environment variable
`TILED_SINGLE_USER_API_KEY` to `secret`, as a placeholder. For actual use, use
a difficult-to-guess secret. Two equally good ways to generate a secure
secret...

With ``openssl``:

    openssl rand -hex 32

With ``python``:

    python -c "import secrets; print(secrets.token_hex(32))"

```

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
python -m tiled.examples.generate_files data/
```

### Quick Start (Not Scalable)

This approach is nice for development and rapid iteration. It indexes the files
at server startup.

```
docker run \
  -p 8000:8000 \
  -e TILED_SINGLE_USER_API_KEY=secret \
  -v ./data:/data:ro \
  ghcr.io/bluesky/tiled:latest \
  tiled serve directory --host 0.0.0.0 /data
```

Two problems with this one-line approach:

* If you restart the server, all the indexing work is re-done from scratch.
* If you horizontally scale with multiple containers, each one will crawl the
  filesystem individually, putting load on the filesystem and potentially getting
  views of the filesystem that are out of sync.

Read on for a scalable approach.

### Scalable to Multiple Processes

Create a place outside the container to store the "catalog", `catalog.db`.

```
mkdir storage/
```

Start the server, potentially multiple on different ports.

```
docker run \
  -p 8000:8000 \
  -e TILED_SINGLE_USER_API_KEY=secret \
  -v ./data:/data:ro \
  -v ./storage:/storage \
  ghcr.io/bluesky/tiled:latest
```

Register the files in the directory `data/` with the catalog.

```
docker run \
  -e TILED_SINGLE_USER_API_KEY=secret \
  -v ./data:/data:ro \
  ghcr.io/bluesky/tiled:latest \
  tiled register http://localhost:8000/ /data --verbose
```

### Scalable to Multiple Hosts

We will use one container to run a database server,
and additional containers to run tiled services.
To connect these services we create a bridge network.

```
docker network create -d bridge tilednet
```

```{note}
We chose to use a custom bridge network here for two reasons:
1. The host network is not supported for Docker Desktop for Mac/Windows
2. The default bridge network does not resolve container DNS entries by hostname
```

Instead of the default SQLite database, we need to use a PostgreSQL database.
One way to run a PostgresSQL database is:

```
export TILED_DATABASE_PASSWORD=db_secret
mkdir postgres-data
docker run -d \
  --name tiled-test-postgres \
  --net=tilednet \
  -p 5432:5432 \
  -e POSTGRES_PASSWORD=${TILED_DATABASE_PASSWORD} \
  -v ./postgres-data:/var/lib/postgresql/data \
  docker.io/postgres:16
```

Initialize the database. (This creates the tables, indexes, and so on used by Tiled.)

```
export TILED_DATABASE_URI=postgresql://postgres:${TILED_DATABASE_PASSWORD}@tiled-test-postgres:5432

docker run --net=tilednet ghcr.io/bluesky/tiled:latest tiled catalog init $TILED_DATABASE_URI
```

Create a directory for Tiled configuration, e.g. `config/`.

```
mkdir config/
```

Place a copy of `example_configs/single_catalog_single_user.yml`, from the Tiled
repository root, in this `config/` directory.

Replace the line:


```yaml
uri: "sqlite:////storage/catalog.db"
```

with a PostgreSQL database URI, such as:

```yaml
uri: "postgresql://postgres:${TILED_DATABASE_PASSWORD}@tiled-test-postgres:5432"
```

Start the server, potentially multiple servers across many hosts.

```
docker run \
  --net=tilednet \
  -p 8000:8000 \
  -e TILED_SINGLE_USER_API_KEY=secret \
  -e TILED_DATABASE_PASSWORD=${TILED_DATABASE_PASSWORD} \
  -v ./config:/deploy/config:ro \
  -v ./data:/data:ro \
  ghcr.io/bluesky/tiled:latest
```

Register the files in the directory `data/` with this catalog.

```
docker run \
  --net=tilednet \
  -e TILED_SINGLE_USER_API_KEY=secret \
  -v ./data:/data:ro \
  ghcr.io/bluesky/tiled:latest \
  tiled register http://localhost:8000/ /data --verbose
```

## Example: Custom configuration

There are configuration examples located in the directory `example_configs`
under the Tiled repository root. The container image has one in particular,
`single_user_single_catalog.yml`, copied into the container under
`/deploy/config/`. Override it by mounting a local directory an
`/deploy/config` as shown:

```
docker run \
  -p 8000:8000 \
  -e TILED_SINGLE_USER_API_KEY=secret \
  -v ./config:/deploy/config:ro \
  ghcr.io/bluesky/tiled:latest
```

You may need to mount additional volumes as well.

## Example: Run Tiled with a dashboard of metrics

See {doc}`../how-to/metrics`.

## Next Steps

See {doc}`../explanations/security` and {doc}`../explanations/access-control`
for examples addressing authentication and authorization.

See {doc}`../reference/service-configuration` for a comprehensive reference.
