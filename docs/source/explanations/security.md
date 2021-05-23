# Security

Tiled employs modern web security standards to enforce access control with
minimum inconvenience to the user.

There are four use cases for the Tiled server.

1. Secure single-user server, analogous to how people use ``jupyter notebook``
2. Public data service
3. Private multi-user data service
4. Multi-user data service with some public and some private content

We address each in turn below.

## Secure single-user server

In this mode, a single user or small group of highly trusted users have access
to a Tiled server protected by a secret access token. This is the default mode.
For those familiar with Jupyter, this is very similar to how
``jupyter notebook`` works.

At startup, a secret token is generated and logged in the terminal.

```
$ tiled serve pyobject tiled.examples.generated_minimal:catalog

    Use the following URL to connect to Tiled:

    "http://127.0.0.1:8000?api_key=a4062c3dd6ab2af0d28fdb7eb278dd985c462ecf08d39f33233554c7fdaa42e7"
```

where the token after ``api_key=`` will be different each time you start the
server. Once you have visited this URL with your web browser or the Tiled Python
client, a cookie will be set in your client and you won’t need to use the token
again. It is valid indefinitely.

For horizontally-scaled deployments where you need multiple instances of the
server to share the same secret, you can set it via an environment variable like
so.

```
TILED_SINGLE_USER_API_KEY=YOUR_SECRET tiled serve ...
```

When the secret is set manually it this way, it is *not* logged in the terminal.

```{note}

Two equally-good ways to generate a secure secret...

With ``openssl``:

    openssl rand -hex 32

With ``python``:

    python -c "import secrets; print(secrets.token_hex(32))"

```

## Public data service

Tiled can serve a public data repository with no authentication required. To
launch it in this mode, use the ``--public`` flag as in

```
tiled serve {pyobject, directory} --public ...
```

or, if using a configuration file as in

```
tiled serve config ...
```

include the configuration:

```yaml
authentication:
    allow_anonymous_access: true
```

When the server is started in this way, it will log a notice like
the following:

```
$ tiled serve pyobject --public tiled.examples.generated_minimal:catalog

    Tiled server is running in "public" mode, permitting open, anonymous access.
    Any data that is not specifically controlled with an access policy
    will be visible to anyone who can connect to this server.

```

## Private multi-user data service

In this mode, users *must* log in to access anything other than the root ``GET /``
and documentation ``GET /docs`` routes.

Tiled is designed to integrate with external user-management systems via a pluggable
Authenticator interface. For those familiar with JupyterHub, these are very
similar to JupyterHub Authenticators. Authenticators fall into two groups:

* Authenticators that accept user credentials directly at the ``POST /token``
  endpoint, following the OAuth2 and OpenAPI standards as shown below, and
  validate the credentials using some underlying authentication mechanism, such
  as PAM.
* Authenticators that use OAuth2 code flow to validate user credentials
  without directly handling them. (No such Authenticators have been written yet
  for Tiled, but track to do so has been laid, closely following the pattern
  established by JupyterHub.)

This mode requires configuration files to be used, as in

```
tiled serve config path/to/config_file(s)
```

in order to specify the Authenticator and, when applicable, any parameters.
The shorthands ``tiled serve pyobject ...`` and ``tiled serve directory ...``
do not currently support this mode. See below for example configurations.

Users can authenticate by POST-ing form-encoded credentials to the ``/token``
endpoint, as in:

```
$ http --form --body :8000/token username=alice password=foo
{
    "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJhbGljZSIsImV4cCI6MTYyMTY0NTk2NH0.Oz65lrf6gYjIgGa4Pf6E8XwjOcRihP4QCR8a8GQN0M4",
    "token_type": "bearer"
}
```

The response body includes an access token with a short (15 minute) lifetime.
Refresh tokens are planned but not yet implemented.

The access tokens are signed using a secret key that, by default, is generated
automatically at server startup. Set the secret manually to ensure that existing
tokens remain valid after a server restart or across horizontally-scaled
deployments of multiple servers.

```{note}

Two equally-good ways to generate a secure secret...

With ``openssl``:

    openssl rand -hex 32

With ``python``:

    python -c "import secrets; print(secrets.token_hex(32))"

```

Apply it by setting the ``TILED_SERVER_SECRET_KEYS`` environment variable, as in:

```
TILED_SERVER_SECRET_KEYS=secret tiled serve ...
```

To rotate keys with a smooth transition, set ``TILED_SERVER_SECRET_KEYS`` to
``;``-separated values, as in

```
TILED_SERVER_SECRET_KEYS=secret1;secret2
```

The first secret value is always used to *encode* new tokens, but all values are
tried to *decode* existing tokens until one works or all fail.

The Python client provides a convenience function for requesting a token
in exchange for credentials.

```{warning}

This interface is experimental and likely to change in a backward-incompatible way.
```

```python
>>> from tiled.client import generate_token, from_uri
>>> token = generate_token("http://localhost:8000")
Username:
Password:
>>> catalog = from_uri("http://localhost:8000", token=token)
```

### Authenticate with local Linux/UNIX users

This requires an additional dependency.

```
pip install pamela
```

The configuration file(s) should include:

```yaml
authentication:
    authenticator: tiled.authenticators:PAMAuthenticator
```

Here is a complete working example:

```yaml
# pam_config.yml
authentication:
    authenticator: tiled.authenticators:PAMAuthenticator
catalogs:
    - path: /
      catalog: tiled.examples.generated_minimal:catalog
```

```
tiled serve config pam_config.yml
```

Authenticate using a system user account and password.

### Toy examples

The ``DictionaryAuthenticator`` authenticates using usernames and passwords
specified directly in the configuration. The passwords may be extracted from
environment variables, as shown. This is not a robust user management system and
should only for used for development and demos.

```yaml
# dictionary_config.yml
authentication:
    authenticator: tiled.authenticators:DictionaryAuthenticator
    args:
        users_to_passwords:
            alice: ${ALICE_PASSWORD}
            bob: ${BOB_PASSWORD}
            cara: ${CARA_PASSWORD}
catalogs:
    - path: /
      catalog: tiled.examples.generated_minimal:catalog
```

```
ALICE_PASSWORD=secret BOB_PASSWORD=secret2 CARA_PASSWORD=secret3 tiled serve config dictionary_config.yml
```

The ``DummyAuthenticator`` accepts *any* username and password combination.

```yaml
# dummy_config.yml
authentication:
    authenticator: tiled.authenticators:DummyAuthenticator
catalogs:
    - path: /
      catalog: tiled.examples.generated_minimal:catalog
```

```
tiled serve config dummy_config.yml
```

To control which users can see which entries in the Catalogs, see
{doc}`access-control`.

## Multi-user data service with some public and some private content

When an Authenticator is used in conjunction with {doc}`access-control`,
certain entries may be designated as "public", visible to any user. By default,
visitors still need to be authenticated (as any user) to see these entries.
To make such entries visible to *anonymous*, unauthenticated users as well,
include the configuration:

```yaml
authentication:
    allow_anonymous_access: true
```