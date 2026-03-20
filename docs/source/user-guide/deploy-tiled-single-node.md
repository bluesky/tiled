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

For development, it can be convenient to use a short memorable secret like
`TILED_SINGLE_USER_API_KEY=secret`. Take caution never to use that approach on
a public-facing server, or on a server containing important data.

**The data and (embedded) databases are inside the container and will not
persist outside it.** Read on to persist it.

### Using Persistent Storage

Create a local directory for data and metadata storage, and mount it
into the container so that it persists after the container stops.

```sh
mkdir storage/

echo "TILED_SINGLE_USER_API_KEY=$(openssl rand -hex 32)" >> .env
docker run --env-file .env -v ./storage:/storage ghcr.io/bluesky/tiled:latest
```

### Customizing Configuration

When you need to introduce custom configurations---such as multi-user
authentication (e.g., OIDC) and access policies or support for custom file
formats---you will need to provide a custom configuration file.

The default configuration used by the container is:

```{literalinclude} ../../../example_configs/config_for_container_file.yml
:language: yaml
:caption: /deploy/config/config.yml
```

You can override it by mounting a configuration directory on the host
to override the configuration directory in the container, by adding
`-v ./my-custom-config-dir:/deploy/config`.

For example, combining persistent storage with a custom configuration:

```sh
docker run --env-file .env \
  -v ./storage:/storage \
  -v ./your/config/directory:/deploy/config \
  ghcr.io/bluesky/tiled:latest
```

### Using Scalable Persistent Storage

The default Tiled container uses embedded databases (SQLite, DuckDB) and
in-process memory for caching. For larger workloads, you can upgrade to
externally-managed services:

- **PostgreSQL** — for metadata and tabular data
- **Redis** — for live data streaming (optional)

Tiled ships with a `compose.yml` to orchestrate these services. Follow the
steps below in order.

1. Create a project directory and add the compose file.

Create a directory for your deployment and place the following `compose.yml`
inside it:

```{literalinclude} ../../../compose.yml
:language: yaml
:caption: compose.yml
```

By default, this uses the configuration file shown above.
When you need to introduce a custom configuration file, place a file named
`compose.override.yml` next to `compose.yml`.

```{literalinclude} ../../../compose.override.example.yml
:language: yaml
:caption: compose.override.yml
```

The name `compose.override.yml` matters: below, `docker-compose` will
automatically apply this override if it detects one is present.

2. Create a `.env` file with secure secrets.

In the same directory, create a `.env` file using the format below as a
template:

```{literalinclude} ../../../.env.example
:language: yaml
:caption: .env
```

Then populate it with generated secrets:

```sh
echo "TILED_SINGLE_USER_API_KEY=$(openssl rand -hex 32)" >> .env
echo "POSTGRES_PASSWORD=$(openssl rand -hex 32)" >> .env
echo "REDIS_PASSWORD=$(openssl rand -hex 32)" >> .env
```

3. Create the database initialization script.

Create an `initdb/` subdirectory and place the following script inside it. This
script initializes the Tiled catalog database (for metadata) and the storage
database (for appendable tabular data) when PostgreSQL first starts.

```{literalinclude} ../../../initdb/01-create-databases.sh
:language: sh
:caption: initdb/01-create-databases.sh
```

4. Start the services.

```sh
docker-compose up -d
```

`docker-compose` will automatically read your `.env` file. To stop all
services, run `docker-compose down`.

```{warning}
Adding `-v` to `docker-compose down` will permanently delete all
persisted storage. Do not use it unless you intend to wipe your data.
```

## Without a Container

### Using Temporary Storage

```sh
tiled serve catalog --temp
```

````{note}

By default, this generates a random API key at startup. For development
purposes, it's convenient to set a fixed API key, to avoid needing to
copy/paste the API key each time. Take caution never to use this approach on a
server that contains important data or is reachable from the public Internet.

```sh
tiled serve catalog --temp --api-key secret
```

````

### Using Persistent Storage

```sh
mkdir storage
mkdir storage/data

tiled serve catalog --init ./storage/catalog.db -w ./storage/data -w duckdb://./storage/data.db
```

## Test the Server

The server is ready to accept requests. You can test it with `curl`,
for example. The landing page `/` and API endpoint `/api/v1/` accept
unauthenticated requests.

```sh
curl 'http://localhost:8000/'  # HTML landing page
curl 'http://localhost:8000/api/v1/'  # REST API
```

Requests that give access to data must be authenticated using
the key configured in the `.env` file.

```sh
curl -H "Authorization:Apikey ${TILED_SINGLE_USER_API_KEY}" 'http://localhost:8000/api/v1/metadata/'
```

To test from a web browser, provide the API key in the URL:
`https://localhost:8000?api_key=...`.

## Next steps

- Notice that the URL uses `http` not `https`. Tiled should be placed behind
  proxy that can perform TLS termination, such as HAproxy, caddy, nginx, or
  Apache.
- For large workloads, multiple instances of Tiled should be deployed,
  sharing the same PostgreSQL, Redis, and network storage volumes for
  a consistent view of the data. This is addressed in the next section.

[Docker]: https://www.docker.com/
[container image]: https://github.com/bluesky/tiled/pkgs/container/tiled
[podman]: https://podman.io/
