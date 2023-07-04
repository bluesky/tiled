# HTTP API

## Standards

Tiled's HTTP API follows the
[OpenAPI v3 specification](https://swagger.io/specification/).
Additionally, it follows the [JSON API standard](https://jsonapi.org/).

When Tiled is configured to use an Authenticator, it adds routes that
comply with the [OAuth2 protocol](https://oauth.net/2/). Specifically,
it implements "sliding sessions" with access and refresh tokens that are
[JWT](https://jwt.io/)s. See {doc}`../explanations/security` for an overview
and {doc}`authentication` for details.

## Overview

The routes are generally spelled like ``GET /{action}/{path}/``, like GitHub
repository URLs, with the path following the structure of the Tree
entries.

The ``GET //metadata/{path}`` route provides the metadata about one node.
The ``GET /node/search/{path}`` route provides paginated access to the children of
a given node, with optional filtering (search). The ``GET /node/full/{path}`` route
provides all the metadata and data below a given node.

Specialized data access routes ``GET /array/block/{path}``, ``GET /array/full/{path}``,
and ``GET /dataframe/partition/{path}`` provide options for slicing and sub-selection
specific to arrays and dataframes. Generic clients, like a web browser,
should use the "full" routes, which send the entire (sliced) result in one
response. More sophisticated clients with some knowledge of Tiled may use the
other routes, which enable parallel chunk-based access.

The root route, `GET /` provides general information about the server and the formats
and authentication providers it supports.

Depending on the server configuration, there may be authentication-related routes
nested under `/auth/`.

## Reference

To view and try the *interactive* docs, visit

[http://tiled-demoblueskyproject.io/docs](http://tiled-demoblueskyproject.io/docs)

or, to work fully locally, start the Tiled server with the demo
Tree from a Terminal

```
tiled serve pyobject --public tiled.examples.generated:tree
```

and navigate your browser to http://localhost:8000/docs.
