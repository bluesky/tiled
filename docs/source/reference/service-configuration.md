---
orphan: true

---

# Service Configuration Reference

This is a comprehensive reference. See also {doc}`../how-to/configuration` for a
practical guide with examples.

The content below is automatically generated from a schema that is used
to validate configuration files when they are read.
(schema_catalogs)=
## catalogs

This section contains a *list* of one or more items,
each describing a Catalog to be served.


(schema_catalogs.catalog)=
### catalogs[item].catalog

Type of catalog to serve. This may be:

- The string `files`, a shorthand for serving a directory of files.
- An import path to a Catalog *instance*.
- An import path to a callable (function or type) that returns a
  Catalog. For these, the `args:` parameter below must be used as well.

In an import path, packages/modules are separated by dots,
and the object itself it separated by a colon.

Examples:

    # Catalog instances
    tiled.examples.generated_minimal:catalog
    tiled examples.generated:demo
    my_python_module:my_catalog_instance

    # Callables that return Catalog instances
    tiled.catalogs.files:Catalog.from_directory
    my_python_module:CustomCatalog


(schema_catalogs.path)=
### catalogs[item].path

URL subpath for to serve this Catalog on.
A good default choice is `"/"` if you are serving
one Catalog.


(schema_catalogs.args)=
### catalogs[item].args

If `catalog:` is set to `files` or some callable that returns a
Catalog, this parameter must be included. It may contain named
arguments to pass to the callable. It may be empty or `null` if the
callable requires no arguments.

Example:

```yaml
- path: "/"
  catalog: tiled.catalogs.files:Catalog.from_directory
  args:
    directory: "path/to/files"
```


(schema_authentication)=
## authentication

This section describes whether and how to authenticate users.

(schema_authentication.authenticator)=
### authentication.authenticator

Type of Authenticator to use.

These are typically from the tiled.authenticators module,
though user-defined ones may be used as well.

This is given as an import path. In an import path, packages/modules
are separated by dots, and the object itself it separated by a colon.

Example:

```yaml
authenticator: tiled.examples.DummyAuthenticator
```


(schema_authentication.args)=
### authentication.args

Named arguments to pass to Authenticator. If there are none,
`args` may be omitted or empty.

Example:

```yaml
authenticator: tiled.examples.PAMAuthenticator
args:
  service: "custom_service"
```


(schema_authentication.secret_keys)=
### authentication.secret_keys

Secret keys used to sign secure tokens.

When generating a secret, is important to produce a difficult-to-guess
random number, and make it different each time you start up a server.
Two equally good ways to generate a secure secret...

With ``openssl``:

    openssl rand -hex 32

With ``python``:

    python -c "import secrets; print(secrets.token_hex(32))"


(schema_authentication.allow_anonymous_access)=
### authentication.allow_anonymous_access

If true, allow unauthenticated, public access to any entries
that are not specifically controlled with an access policy.

Default is false.


(schema_authentication.single_user_api_key)=
### authentication.single_user_api_key

Secret API key used in single-user deployments.

When generating a secret, is important to produce a difficult-to-guess
random number, and make it different each time you start up a server.
Two equally good ways to generate a secure secret...

With ``openssl``:

    openssl rand -hex 32

With ``python``:

    python -c "import secrets; print(secrets.token_hex(32))"
  


(schema_authentication.access_token_max_age)=
### authentication.access_token_max_age

This controls how often fresh access token have to be
re-issued. The process is transparent to the user and just affects performance.
An *access* token cannot be revoked, so its lifetime should be short. The
default is 900 seconds (15 minutes).

Units are **seconds**.


(schema_authentication.refresh_token_max_age)=
### authentication.refresh_token_max_age

Time after which inactive sessions
(sessions that have not refreshed tokens) will time out.
Default is

Units are **seconds**.


(schema_authentication.session_max_age)=
### authentication.session_max_age

Even *active* sessions are timed out after this
limit, and the user is required to resubmit credentials. By default,
this is unset and active session are never shut down.


(schema_allow_origins)=
## allow_origins

This list of domains enables web apps served from other domains to make
requests to the tiled serve.

Example:

```yaml
allow_origins:
  - https://chart-studio.plotly.com
```

Read more about Cross-Origin Resource Sharing (CORS)
from Mozilla's web developer documentation.

https://developer.mozilla.org/en-US/docs/Web/HTTP/CORS

