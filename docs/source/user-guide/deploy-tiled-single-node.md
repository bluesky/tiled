(deploy-tiled-single-node)=
# Deploy Tiled on a Single Host

As a matter of preference, you may decide to run the Tiled server using a
container (e.g. [Docker][], [Podman][]). This documentation is organized into
two sections, illustrating how to deploy Tiled with or without a container.

## With a Container

### Using Temporary Storage

Generate a secure secret and start a Tiled server from the [container image][].

```sh
echo "TILED_SINGLE_USER_API_KEY=$(openssl rand -hex 32)" >> .env
docker run --env-file .env ghcr.io/bluesky/tiled:latest
```

**The data and (embedded) databases are inside the container and will not
persist outside it.** Read on to persist it.

### Using Persistent Storage

Create a local directory for data and metadata storage, and mount it
into the container so that it persist after the container stops.

```sh
mkdir storage/

echo "TILED_SINGLE_USER_API_KEY=$(openssl rand -hex 32)" >> .env
docker run --env-file .env -v ./storage:/storage ghcr.io/bluesky/tiled:latest
```

### Using Scalable Persistent Storage

The default configuration of the Tiled container image above does not require any
externally-managed services. It runs on "embedded" databases (SQLite, DuckDB)
and file-based data storage. It caches recent metadata and (small) data for
live-streaming in the memory of the server process.

To scale Tiled for larger workloads, you must upgrade from this simple
single-process configuration to one that employs externally-managed services:

- PostgreSQL for metadata and tabular data
- Redis (optional, needed for streaming)

Tiled ships with a compose file to do this:

```{literalinclude} ../../../compose.yml
:language: yaml
:caption: compose.yml
```

Place the above `compose.yml` in a directory, and create
create a `.env` file next to it, formatted like so.

```{literalinclude} ../../../.env.example
:language: yaml
:caption: .env
```

This snippet can populate the file with secure secrets for you:

```sh
echo "TILED_SINGLE_USER_API_KEY=$(openssl rand -hex 32)" >> .env
echo "POSTGRES_PASSWORD=$(openssl rand -hex 32)" >> .env
echo "REDIS_PASSWORD=$(openssl rand -hex 32)" >> .env
```

Create a directory `initdb` and place this file within it.
This creates a Tiled "catalog" database for metadata and a Tiled
"storage" database for appendable tabular data.

```{literalinclude} ../../../initdb/01-create-databases.sh
:language: sh
:caption: initdb/01-create-databases.sh
```

Finally, start the services like so. (It looks for a `.env` file automatically.)

```sh
docker-compose up -d
```

As usual, `docker-compose down` stops the services.

## Without a Container

### Using Temporary Storage

```sh
tiled serve catalog --temp
```

````{note}

By default, this generates a random API key at startup.  For development
purposes, it's convenient to set a fixed API key, to avoid needing to
copy/paste the API key each time. Never use this approach on a server
that contains important data or is reachable from the public Internet.

```sh
tiled serve catalog --temp --api-key secret
```

````

### Using Persistent Storage

```sh
mkdir storage
mkdir storage/data

tiled serve catalog ./storage/catalog.db -w ./storage/data -w duckdb://./storage/data.db
```

[Docker]: https://www.docker.com/
[container image]: https://github.com/bluesky/tiled/pkgs/container/tiled
[podman]: https://podman.io/
