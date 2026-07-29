# Serve Tiled Behind a Reverse Proxy

Tiled can be served on a URL path prefix, such as
`https://example.com/tiled/`, rather than at the root of a domain. Three
pieces have to agree.

## 1. The proxy strips the prefix

Tiled's routes are always registered at the root (`/api/v1/...`, `/ui/...`).
Setting `root_path` does not mount them under the prefix: it tells Tiled what
prefix the *client* sees, so that the URLs it generates are correct. The proxy
must therefore remove the prefix before forwarding, or every request 404s.

```nginx
location /tiled/ {
    proxy_pass http://127.0.0.1:8000/;  # the trailing slash strips /tiled/

    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-Host $host;
    proxy_set_header X-Forwarded-Proto $scheme;

    # Required for the streaming API (/api/v1/stream/...), which uses
    # websockets. Without these the handshake fails.
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
}
```

`location /tiled/` does not match a bare `https://example.com/tiled`. Add a
redirect if you want that to work:

```nginx
location = /tiled {
    return 301 /tiled/;
}
```

## 2. The proxy sets `X-Forwarded-Host` and `X-Forwarded-Proto`

Tiled builds the links in its responses from these headers. Without them, the
links point at the internal hostname and scheme (for example
`http://127.0.0.1:8000/...`) instead of the address the client used. See also
the `uvicorn.proxy_headers` and `uvicorn.forwarded_allow_ips` settings in
{doc}`../reference/service-configuration`.

## 3. Tiled is configured with a matching `root_path`

```yaml
uvicorn:
  root_path: /tiled
```

This tells Tiled which prefix the client sees. Without it the web UI loads its
assets from `/ui/` and every asset 404s.

## Migrating from `TILED_BUILD_PUBLIC_PATH`

The web UI used to be built with its base path baked in at build time via the
`TILED_BUILD_PUBLIC_PATH` environment variable, which meant a given build could
only be served under one prefix. That variable is now ignored: the base path is
injected per request from `root_path`, so one build serves any prefix. Remove
`TILED_BUILD_PUBLIC_PATH` from your build and set `uvicorn.root_path` instead.
