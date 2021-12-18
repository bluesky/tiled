# HTTP API

## Standards

Tiled's HTTP API follows the
[OpenAPI v3 specification](https://swagger.io/specification/).
Additionally, it follows the conventions of the
[JSON API standard](https://jsonapi.org/).

When Tiled is configured to use an Authenticator, it adds routes that
comply with the [OAuth2 protocol](https://oauth.net/2/). Specifically,
it implements "sliding sessions" with access and refresh tokens that are
[JWT](https://jwt.io/)s. See {doc}`../explanations/security` for an overview
and {doc}`authentication` for details.

## Overview

The routes are generally spelled like ``GET /{action}/{path}/``, like GitHub
repository URLs, with the path following the structure of the Tree
entries.

the metadata about each entry. The ``GET /node/search`` route provides
paginated access to a node, with optional filtering (search).

The ``GET /node/metadata`` route provides the metadata about one node.

The data access routes like ``GET /array/block`` and ``GET /array/full`` are
designed to different kinds of clients. Both support slicing / sub-selection
as appropriate to the data structure. Generic clients, like a web browser,
should use the "full" route, which sends the entire (sliced) result in one
response. More sophisticated clients that can reassemble tiled results should
use the other routes, which support efficient chunk-based access.

The ``POST /token`` route accepts form-encoded credentials and responds with
an access token and a refresh token. The ``POST /token/refresh`` route accepts a
refresh token and responds with a new set of tokens.

## Reference

To view and try the *interactive* docs, start the Tiled server with the demo
Tree from a Terminal

```
tiled serve pyobject --public tiled.examples.generated:tree
```

and navigate your browser to http://localhost:8000/docs.

A non-interactive reference with the same content follow below.

```{eval-rst}
.. openapi:: api.yml
```
