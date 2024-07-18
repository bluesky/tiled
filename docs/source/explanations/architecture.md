# Architecture

This is an overview of the major components of Tiled.

(client-arch)=
## Client

Tiled ships with a Python client. This is separable from the rest of the
project, and could potentially someday be split off as a separate package. The
client enables the user to navigate and access the data in a Tiled server using
familiar Python item-lookup and slicing idioms, while it generates HTTP
requests to transfer metadata and data.

(connection-context-arch)=
### Connection _Context_
When the user connects, a **Context** object is created. The Context is
shared by all client-side objects that use this connection. It wraps an
[httpx HTTP client][httpx], which in turn wraps an HTTP connection pool and (if
applicable) authentication-related state---either an API key or a pair of
OAuth2 access and refresh tokens.

Further Reading:
* [`Context` reference](#context-ref)

(client-side-caching-arch)=
### Client-side _Caching_
The Context also may hold an HTTP response **Cache**, similar to a web
browser's cache. This is currently not enabled by default because it is
experimental.

Further Reading:
* [Client HTTP Response Cache overview](#client-http-response-cache)
* [`Cache` reference](#client-http-response-cache-ref)

(server-arch)=
## Server

The Tiled HTTP server is built using the framework [FastAPI][], which is built
on [Starlette][]. A key feature of FastAPI is auto-generated [OpenAPI][]
documentation, which Tiled serves at the `GET /docs` endpoint. FastAPI
works with [Pydantic][] to parse and validate requests.

(authentication-arch)=
### Authentication
Most endpoints require authentication, unless the server is configured to be
public. For single-user deployments, a single API key is specified or randomly
generated at server startup. For multi-user deployments, an **Authentication
Database** (PostgreSQL or SQLite) is used to store session information and to
validate API keys.

Further Reading:
* [Security](#security)
* [Login Tutorial](#login-tutorial)
* [Authentication Details](#auth-details)
* [Tiled Authentication Database](#tiled-authn-database)

(accessing-data-and-metadata-arch)=
### Accessing Data and Metadata
Endpoints that serve metadata or data resolve the URL path to identify the
relevant [**Adapter**](#adapter-arch), which returns the data
as a scientific data structure. It may be a "lazy" data structure,
representing data that will be loaded later -- on demand and piecemeal.

(content-negotiation-and-serializers-arch)=
### Content Negotiation and _Serializers_
The endpoint implements [content negotiation][], comparing the list of requested formats
that are accepted by the client to those supported by the server for this particular
dataset. It dispatches to a registry of **Serializers**, which convert the data
structure into bytes which can be sent in a response by FastAPI. Custom
serializers may be registered during server configuration.

Further Reading:
* [Custom Export Formats](#custom-export-formats)
* [Media Type Format Registry reference](#media-type-registry-ref)

(compression-arch)=
### Compression
On its way, the response may be compressed, again using content negotiation
to compare the list of compression schemes supported by the client (if any)
to those the server deems appropriate to the dataset.

(adapter-arch)=
## Adapter

In Tiled, an **Adapter** provides a standard interface to data, regardless of
how it is stored. Adapters that wrap different structure have different
interfaces. For example, an array adapter implements `read_block` whereas a
table adapter implements `read_partition`. But all array adapters are alike,
and all table adapters are alike. They enable the server to abstract over
the details of _how_ to get the data.

Typically Adapters take a filepath or URI and, perhaps, some additional
configuration. For development, test, or demonstration purposes, Adapters can wrap
in-memory data, such as a numpy array. Several Adapters are included in the Tiled
codebase, in the spirit of "batteries included," but Adapters can be defined in
external modules, too, and operate on the same footing as the built-in ones.

Further Reading:
* [Adapters](#adapters-ref)

(catalog-arch)=
## Catalog

The Catalog is an Adapter that stores the metadata and structure for a
potentially large number of datasets in a SQL database (PostgreSQL or SQLite).
This enables it to efficiently respond to _metadata_ requests and _search_
requests without opening any data files. Requests for _data_ are then
dispatched down to the appropriate [Adapter](#adapter-arch) which can load the data
from the given storage medium and format.

Not all Tiled servers are configured to use the Catalog:

* Demo deployments wrapping a handful of datasets use data Adapters directly,
  with no need for a database.
* Specialized deployments with custom Adapters may use a custom database or
  external service.

But for most standard applications, including serving a directory of files or
providing a writable data store, the Catalog is used.

See {doc}`catalog` for an explanation of the database.

[FastAPI]: https://fastapi.tiangolo.com/
[httpx]: https://www.python-httpx.org/
[Starlette]: https://www.starlette.io/
[OpenAPI]: https://www.openapis.org/
[Pydantic]: https://docs.pydantic.dev/
[content negotiation]: https://developer.mozilla.org/en-US/docs/Web/HTTP/Content_negotiation
