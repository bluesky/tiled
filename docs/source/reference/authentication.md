# Authentication

See {doc}`../explanations/security` for an overview.
This page addresses technical details relevant to:

* Customizing the various lifetimes (timeouts) in the system
* Horizontally scaling deployments
* Writing new clients

## Configure session lifetime parameters

The server implementing "sliding sessions". The following are tunable:

* Maximum inactive session age --- Time after which inactive sessions
  (sessions that have not refreshed tokens) will time out.
* Maximum session age --- Even *active* sessions are timed out after this
  limit, and the user is required to resubmit credentials. By default,
  this is unset and active session are never shut down.
* Access token max age --- This is a largely internal detail that has
  negligible impact on the user. It should be short because an
  access token cannot be revoked.

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

## Client Authentication Walk-through

Let's walk through the authentication flow by example. Start a server like so:

```
$ TILED_ACCESS_TOKEN_MAX_AGE=20 \
> ALICE_PASSWORD=secret1 \
> BOB_PASSWORD=secret2 \
> CARA_PASSWORD=secret3 \
> tiled serve config example_configs/toy_authentication.yml 
```

We are configuring the timeout for access tokens to be unrealistically short (20
seconds) for demonstration purposes.

Here it is again in one line, for convenient copy/paste:

```
TILED_ACCESS_TOKEN_MAX_AGE=20 ALICE_PASSWORD=secret1 BOB_PASSWORD=secret2 CARA_PASSWORD=secret3 tiled serve config example_configs/toy_authentication.yml 
```

To make requests, we'll use [httpie](https://httpie.io/)
and then extract the portions of interest with
[jq](https://stedolan.github.io/jq/). You could do the same with old-fashioned
`curl` and copy/paste; these are just conveniences.


This server is configured to be private, so unauthenticated users cannot see the
``/metadata/`` route, for example.

```
$ http --session=./session.json GET :8000/metadata/  # not allowed!
HTTP/1.1 401 Unauthorized
content-length: 30
content-type: application/json
date: Tue, 25 May 2021 16:48:29 GMT
server: uvicorn
server-timing: app;dur=2.7

{
    "detail": "Not authenticated"
}
```

Users can authenticate by POST-ing form-encoded credentials to the ``/token``
endpoint, as in:

```
$ http --session=./session.json --form POST http://127.0.0.1:8000/token username=alice password=secret1 > tokens.json

$ jq . tokens.json
{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJhbGljZSIsImV4cCI6MTYyMTk2MTMzNCwidHlwZSI6ImFjY2VzcyJ9.ihZRpS-dPZrMinoWMW7Ox_9mUUTN-KS05Lzcg5pFWOM",
  "expires_in": 20,  # configured artificially short (20 seconds) for this demo
  "refresh_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJhbGljZSIsInR5cGUiOiJyZWZyZXNoIiwiaWF0IjoxNjIxOTc1NzE0LjUzNDEwOCwic2lkIjozNjgwMzkyMTUwMjY4NDY3ODY5NTk0ODk0MDcwNjE1MTA4NzczNiwic2N0IjoxNjIxOTc1NzE0LjUzNDEwOH0.4BzBUYGhL7fEoK8mBYP67ypB7aPTSEmmkFRiOSfzS58",
  "refresh_token_expires_in": 604800,
  "token_type": "bearer"
}
```

The response body includes an access token with a short lifetime (default 15
minutes) and a refresh token with a long lifetime (default one week).

Now that we have an `access_token` for the user `alice`, we can see the entries
that `alice` is allowed to see. (This only works for 20 seconds after we run the
command above. If you are slow on the draw, you may need to run the previous command again,
or else increase `TILED_ACCESS_TOKEN_MAX_AGE`.)

```
$ http --session=./session.json GET :8000/metadata/ Authorization:"Bearer $(jq -r .access_token tokens.json)"
HTTP/1.1 200 OK
content-length: 214
content-type: application/json
date: Tue, 25 May 2021 16:48:47 GMT
etag: f86de76282c7b77dd1efc28d4fe8b6fe
server: uvicorn
server-timing: app;dur=3.3

{
    "data": {
        "attributes": {
            "client_type_hint": null,
            "count": 2,
            "metadata": {},
            "sorting": null
        },
        "id": "",
        "links": {
            "self": "http://localhost:8000/metadata/"
        },
        "meta": null,
        "type": "catalog"
    },
    "error": null,
    "links": null,
    "meta": null
}
```

If we wait for `TILED_ACCESS_TOKEN_MAX_AGE` to pass, we find that we are locked out again.

```
$ http --session=./session.json GET :8000/metadata/ Authorization:"Bearer $(jq -r .access_token tokens.json)"
HTTP/1.1 401 Unauthorized
content-length: 38
content-type: application/json
date: Tue, 25 May 2021 16:49:02 GMT
server: uvicorn
server-timing: app;dur=2.6

{
    "detail": "Access token has expired."
}
```

But we can use a refresh token to request a new access token. 

```
$ http --session=./session.json POST http://127.0.0.1:8000/token/refresh refresh_token=$(jq -r .refresh_token tokens.json) x-tiled-csrf-token:$(jq -r .cookies.tiled_csrf_token.value session.json) > new_tokens.json

$ jq . new_tokens.json
{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJhbGljZSIsImV4cCI6MTYyMTk2MTM4MywidHlwZSI6ImFjY2VzcyJ9.AfwuR7sVqOEwkOpveTyKYCBaqETO2fEnWm8f-KFxuVA",
  "expires_in": 20,
  "refresh_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJhbGljZSIsInR5cGUiOiJyZWZyZXNoIiwiaWF0IjoxNjIxOTc1NzYzLjU4MzQxNCwic2lkIjozNjgwMzkyMTUwMjY4NDY3ODY5NTk0ODk0MDcwNjE1MTA4NzczNiwic2N0IjoxNjIxOTc1NzE0LjUzNDEwOH0.bCfROz_j2DkkbTNgpDgYAOdOJTk1itDSPcxT17Xhl9U",
  "refresh_token_expires_in": 604800,
  "token_type": "bearer"
}
```

We have a new `access_token` and also a new `refresh_token`. This is the
"sliding session". As long as we remain active, refreshing the token, our
session will stay alive and we will not need to resubmit our credentials.
There is an optional, configurable maximum age after which even
*active* sessions will be timed out.

(The `csrf`-related parameters above are implementing a standard protection
against cross-site forgery,
[Double Submit Cookie](https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html#double-submit-cookie).)

With our fresh `access_token`, we can access the data again:

```
$ http --session=./session.json GET :8000/metadata/ Authorization:"Bearer $(jq -r .access_token new_tokens.json)"HTTP/1.1 200 OK
content-length: 214
content-type: application/json
date: Tue, 25 May 2021 16:49:36 GMT
etag: f86de76282c7b77dd1efc28d4fe8b6fe
server: uvicorn
server-timing: app;dur=3.3

{
    "data": {
        "attributes": {
            "client_type_hint": null,
            "count": 2,
            "metadata": {},
            "sorting": null
        },
        "id": "",
        "links": {
            "self": "http://localhost:8000/metadata/"
        },
        "meta": null,
        "type": "catalog"
    },
    "error": null,
    "links": null,
    "meta": null
}
```