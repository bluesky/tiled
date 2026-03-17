(deploy-tiled-single-node)=
# Deploy Tiled on a Single Host

As a matter of preference, you may decide to run the Tiled server using a
container (e.g. [Docker][], [Podman][]). This documentation is organized into
two sections, illustrating how to deploy Tiled with or without a container.

## With a Container

A [Tiled container image][] is published in the GitHub container registry.

```sh
docker pull ghcr.io/bluesky/tiled:latest
```

### Configure a single-user API key

```{note}
In these examples we run a server that is secured with a single secret key.
But Tiled can also be configured for **multi-user** deployments, integrating
with external identity providers (e.g., ORCID, Google, ....).

See {doc}`../explanations/security` and {doc}`../explanations/access-control`.
```

Generate a secret in one of these two ways.

With ``openssl``:

```sh
export TILED_SINGLE_USER_API_KEY=$(openssl rand -hex 32)
```

With ``python``:

```sh
export TILED_SINGLE_USER_API_KEY=$(python -c 'import secrets; print(secrets.token_hex(32))')
```

### Using Temporary Storage

```sh
docker run \
  -p 8000:8000 \
  -e TILED_SINGLE_USER_API_KEY=$TILED_SINGLE_USER_API_KEY \
  ghcr.io/bluesky/tiled:latest
```

**The data and database are inside the container and will not persist outside
it.** Read on to persist it.


### Using Persistent Storage

```sh
mkdir storage/

docker run \
  -p 8000:8000 \
  -e TILED_SINGLE_USER_API_KEY=$TILED_SINGLE_USER_API_KEY \
  -v ./storage:/storage ghcr.io/bluesky/tiled:latest
```

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

tiled serve catalog storage/catalog.db -w storage/data -w duckdb://storage/data
```

[Docker]: https://www.docker.com/
[Tiled container image]: https://github.com/bluesky/tiled/pkgs/container/tiled
[podman]: https://podman.io/
