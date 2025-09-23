(auth-details)=
# Authentication Details

See {doc}`../explanations/security` for an overview.
This page addresses technical details relevant to:

- Client authentication flow
* Customizing the various lifetimes (timeouts) in the system
* Horizontally scaling deployments

## Client authentication flow

In this section we'll use the command-line HTTP client
[httpie](https://httie.io/) and [jq](https://stedolan.github.io/jq/) to parse
the JSON responses.

Some Tiled servers are configured handle credentials directly. Others are
configured to refer the user to a web browser to authenticate with a third party
(e.g. ORCID, Google) and return to Tiled with a token. We will demonstrate each
in turn.

To test the first kind, we'll start a tiled server with a demo "toy"
authentication system to work against. Use this example configuration that
is included with the tiled source code, and start a server like so.

```{eval-rst}
.. literalinclude:: ../../../example_configs/toy_authentication.yml
   :caption: example_configs/toy_authentication.py
```

Note that you will need to run these helper tools to prep the backing databases that Tiled needs:
```
# prep the access tags and catalog databases
python example_configs/access_tags/compile_tags.py
python example_configs/catalog/create_catalog.py
```

then, you can launch the server:
```
ALICE_PASSWORD=secret1 BOB_PASSWORD=secret2 CARA_PASSWORD=secret3 tiled serve config example_configs/toy_authentication.yml
```

To test the second kind, we'll use `https://tiled-demo.blueskyproject.io`, which
is configured to use [ORCID](https://orcid.org/) for authentication.

### Scenario 1: Authenticator Directly Handles Credentials

An initial handshake with the `/` route tells us that authentication is required
on this server. This is one authentication provider, and it expects (HTTP
basic) password authentication. The `auth_endpoint` tells us where to POST our
credentials.

```
$ http :8000/api/v1/ | jq .authentication
{
  "required": true,
  "providers": [
    {
      "provider": "toy",
      "mode": "internal",
      "links": {
        "auth_endpoint": "http://localhost:8000/api/v1/auth/provider/toy/token"
      },
      "confirmation_message": null
    }
  ],
  "links": {
    "whoami": "http://localhost:8000/api/v1/auth/whoami",
    "apikey": "http://localhost:8000/api/v1/auth/apikey",
    "refresh_session": "http://localhost:8000/api/v1/auth/session/refresh",
    "revoke_session": "http://localhost:8000/api/v1/auth/session/revoke/{session_id}",
    "logout": "http://localhost:8000/api/v1/auth/logout"
  }
}
```

Exchange username/password credentials for "access" and "refresh" tokens.

```
$ http --form POST :8000/api/v1/auth/provider/toy/token username=alice password=secret1 > tokens.json
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
$ http GET :8000/api/v1/metadata/ "Authorization:Bearer `jq -r .access_token tokens.json`"
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
            "search": "http://localhost:8000/api/v1/search/",
            "self": "http://localhost:8000/api/v1/metadata/"
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
$ http GET :8000/api/v1/metadata/ "Authorization:Bearer `jq -r .access_token tokens.json`"
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
$ http POST :8000/api/v1/auth/session/refresh refresh_token=`jq -r .refresh_token tokens.json` > tokens.json
```

And resume making requests with the new access token.

To experiment with token expiry and renewal, it can be useful to tune the various
"max age" parameters very low---10 seconds or so. The next section describes how
to configure these parameters.

### Scenario 2: Authenticator Refers to a Third-Party Identity Provider

An initial handshake with the `/` route tells us that this server uses
`"external"` authentication.

```
$ http https://tiled-demo.blueskyproject.io/api/v1/ | jq .authentication.type
"external"
```

Elsewhere in this same response, we can find the authentication endpoint for
this external identity provider.

```
$ http https://tiled-demo.blueskyproject.io/api/v1/ | jq .authentication.endpoint
"https://orcid.org/oauth/authorize?client_id=APP-0ROS9DU5F717F7XN&response_type=code&scope=openid&redirect_uri=https://tiled-demo.blueskyproject.io/auth/code",
```

Navigate to this address in a web browser, log in when prompted, and authorize
Tiled when prompted.  You will be redirected to a page at
`https://tiled-demo.blueskyproject.io/auth/code?code=[redacted]` and shown
a valid refresh token from Tiled that encodes your ORCID username. Exchange the
refresh token for an access token and a fresh refresh token like so.

```
$ http POST https://tiled-demo.blueskyproject.io/api/v1/auth/session/refresh refresh_token="TOKEN PASTED FROM WEB BROWSER" > tokens.json
```

From here, everything follows the same as in Scenario 1, above.

## Configure session lifetime parameters

The server implements "sliding sessions". The following are tunable:

* Maximum inactive session age --- Time after which inactive sessions
  (sessions that have not refreshed tokens) will time out.
* Maximum session age --- Even *active* sessions are timed out after this
  limit, and the user is required to resubmit credentials.
* Access token max age --- This controls how often fresh access token have to be
  re-issued. The process is transparent to the user and just affects performance.
  An *access* token cannot be revoked, so its lifetime should be short. The
  default is 15 minutes.

These are tuned, respectively, by the following configuration parameters,
given in units of seconds. The default values are shown.

```yaml
authentication:
    refresh_token_max_age: 604800  # one week
    session_max_age: 31536000  # 365 days
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

or by setting the ``TILED_SECRET_KEYS`` environment variable.

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

or set ``TILED_SECRET_KEYS`` as a json list, e.g.

```
TILED_SECRET_KEYS='["NEW_SECRET", "OLD_SECRET"]'
```

The first secret value is always used to *encode* new tokens, but all values are
tried to *decode* existing tokens until one works or all fail.
