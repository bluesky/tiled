# Authentication Details

See {doc}`../explanations/security` for an overview.
This page addresses technical details relevant to:

- Client authentcation flow
* Customizing the various lifetimes (timeouts) in the system
* Horizontally scaling deployments

## Client authentcation flow

In this section we'll use the command-line HTTP client
[httpie](https://httie.io/) and [jq](https://stedolan.github.io/jq/) to parse
the JSON responses.

We'll start a tiled server with a demo "toy" authentcation system to work
against. Save the following `config.yaml` and start a server like so.

```{eval-rst}
.. literalinclude:: ../../../example_configs/toy_authentication.yml
```

```
ALICE_PASSWORD=secret1 BOB_PASSWORD=secret2 CARA_PASSWORD=secret3 tiled serve config config.yml
```

Exchange username/password credentials for "access" and "refresh" tokens.

```
$ http --form POST :8000/token username=alice password=secret1 > tokens.json
```

The content of `tokens.json` looks like

```json
{"access_token":"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJhbGljZSIsImV4cCI6MTYyODEwNTQyMiwidHlwZSI6ImFjY2VzcyJ9.bd8T3yYo9LDxBaCB3luSbSBh4dcVJDfXTFtW9s6aa3Q",
 "expires_in":900,
 "refresh_token":"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJhbGljZSIsInR5cGUiOiJyZWZyZXNoIiwiaWF0IjoxNjI4MTE4OTIyLjU3NTM4NSwic2lkIjoxMzgwNjIwMjE2MTg3ODQyMTM2NzgwNzQ2NjEwNzE3NTAxMzEyNTMsInNjdCI6MTYyODExODkyMi41NzUzODV9.ms0y8x4csMVvyDozCCa2RE48nRDEd16RFK9RbrsBS5E",
 "refresh_token_expires_in":604800,
 "token_type":"bearer"
}
```

Make an authenticated request using that access token.

```
$ http GET :8000/metadata/ "Authorization:Bearer `jq -r .access_token tokens.json`"
HTTP/1.1 200 OK
content-length: 239
content-type: application/json
date: Wed, 04 Aug 2021 19:17:56 GMT
etag: c1f7b0169f6baabad75f80a0bf6a2656
server: uvicorn
server-timing: app;dur=3.9
set-cookie: tiled_csrf=1-Cpa1WcwggakZ91FtNsscjM8VO1N1znmuILlL5hGY8; HttpOnly; Path=/; SameSite=lax

{
    "data": {
        "attributes": {
            "count": 2,
            "metadata": {},
            "sorting": null,
            "specs": null
        },
        "id": "",
        "links": {
            "search": "http://localhost:8000/search/",
            "self": "http://localhost:8000/metadata/"
        },
        "meta": null,
        "type": "tree"
    },
    "error": null,
    "links": null,
    "meta": {}
}
```

When the access token expires (after 15 minutes, by default) requests will be
rejected like this.

```
$ http GET :8000/metadata/ "Authorization:Bearer `jq -r .access_token tokens.json`"
HTTP/1.1 401 Unauthorized
content-length: 53
content-type: application/json
date: Wed, 04 Aug 2021 19:22:07 GMT
server: uvicorn
server-timing: app;dur=2.7
set-cookie: tiled_csrf=6sPHOrjBRzZOiSuXOXNtaDNyNNeqQj86nPIXf7X3C1M; HttpOnly; Path=/; SameSite=lax

{
    "detail": "Access token has expired. Refresh token."
}
```

Exchange the refresh token for a fresh pair of access and refresh tokens.

```
$ http POST :8000/token/refresh refresh_token=`jq -r .refresh_token tokens.json` > tokens.json
```

And resume making requests with the new access token.


# Configure session lifetime parameters

The server implements "sliding sessions". The following are tunable:

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

See also {doc}`service-configuration`.

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