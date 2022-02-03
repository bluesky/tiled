# Standards Used by Tiled

Tiled leverages the following standards from the web and from scientific computing.

* [OpenAPI](https://www.openapis.org/) to describe its HTTP API
* [ASGI](https://asgi.readthedocs.io/en/latest/) to interoperate with asynchronous Python web components
* [Content negotiation](https://developer.mozilla.org/en-US/docs/Web/HTTP/Content_negotiation) to determine what format and what if any compression encoding to use
* [MIME types](https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types) to describe formats
* [Cache Control](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Cache-Control) directives to manage caching resources
* [OAuth2](https://oauth.net/2/) Password Flow, [OpenID Connect](https://openid.net/connect/) with OAuth2 Code Flow, and/or [SAML](https://developers.onelogin.com/saml) for authentication
* OAuth2 refresh flow with [JWTs](https://jwt.io/)
* [Prometheus](https://prometheus.io/) metrics for server observability
* [Apache Arrow](https://arrow.apache.org/) to describe tabular data
* The [Numpy Array Interface](https://numpy.org/doc/stable/reference/arrays.interface.html#object.__array_interface__) to describe N-dimensional strided array data
* [JSON Schema](https://json-schema.org/understanding-json-schema/) to describe the HTTP API (via OpenAPI) and to document and validate server- and client-side configuration files
