# Authentication

See {doc}`../explanations/security` for an overview.
This page addresses technical details relevant to:

* Customizing the various lifetimes (timeouts) in the system
* Horizontally scaling deployments

## Configure session lifetime parameters

The server implementing "sliding sessions". The following are tunable:

* Maximum inactive session age --- Time after which inactive sessions
  (sessions that have not refreshed tokens) will time out.
* Maximum session age --- Even *active* sessions are timed out after this
  limit, and the user is required to resubmit credentials. By default,
  this is unset and active session are never shut down.
* Access token max age --- This controls how often fresh access token have to be
  re-issued. The process is transparent to the user and just affects performance.
  An *access* token cannot be revoked, so its lifetime should be short. The
  default is 15 minutes.

These are tuned, respectively, by the following configuration parameters,
given in units of seconds. The default values are shown.

```yaml
authentication:
    refresh_token_max_age: 604800  # one week
    session_max_age: None  # unlimited
    access_token_max_age: 900  # 15 minutes
```

and may also be set via the environment:

```
TILED_REFRESH_TOKEN_MAX_AGE
TILED_SESSION_MAX_AGE
TILED_ACCESS_TOKEN_MAX_AGE
```

## Set and Rotate the Signing Key

The access tokens are signed using a secret key that, by default, is generated
automatically at server startup. **Set the secret manually to ensure that
existing tokens remain valid after a server restart or across
horizontally-scaled deployments of multiple servers.**

```{note}

When generating a secret, is important to produce a difficult-to-guess random
number, and make it different each time you start up a server.  Two equally good
ways to generate a secure secret...

With ``openssl``:

    openssl rand -hex 32

With ``python``:

    python -c "import secrets; print(secrets.token_hex(32))"

```

Apply it by including the configuration

```yaml
authentication:
    secret_keys:
        - "SECRET"
```

or by setting the ``TILED_SERVER_SECRET_KEYS``.

If you prefer, you can extract the keys from the environment like:

```yaml
authentication:
    secret_keys:
        - "${SECRET}"  # will be replaced by the environment variable
```

To rotate keys with a smooth transition, provide multiple keys

```yaml
authentication:
    secret_keys:
        - "NEW_SECRET"
        - "OLD_SECRET"
```

or set ``TILED_SERVER_SECRET_KEYS`` to ``;``-separated values, as in

```
TILED_SERVER_SECRET_KEYS=NEW_SECRET;OLD_SECRET
```

The first secret value is always used to *encode* new tokens, but all values are
tried to *decode* existing tokens until one works or all fail.