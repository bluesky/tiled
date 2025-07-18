# Python Client Authentication

This covers authentication from the user (client) perspective. To learn how to
_deploy_ authenticated Tiled servers, see {doc}`../explanations/security`.

## Interactive Login

Some Tiled servers are configured to let users connect anonymously without
authenticating.

```py
>>> from tiled.client import from_uri
>>> client = from_uri("https://...")
>>> <Container ...>
```

Logging in may enable you to see more datasets that may not be public.
Log in works in one of two ways, depending on the server.

1. Username and password ("OAuth2 password grant")

   ```py
   >>> client.login()
   Username: ...
   Password:
   ```

2. Via a web browser ("OAuth2 device code grant")

   ```py
   >>> client.login()
   You have 15 minutes visit this URL

   https://...

   and enter the code: XXXX-XXXX
   ```

In the future, Tiled will log you into this server automatically, without
re-prompting for credentials, until your session expires.

   ```py
   >>> from tiled.client import from_uri
   >>> client = from_uri("https://...")
   # Automatically logged in!

   # This is a quick way to verify whether you are already logged in
   >>> client.context
   <Context authenticated as '...'>
   ```

To opt out of this, set `remember_me=False`:

```py
>>> from tiled.client import from_uri
>>> client = from_uri("https://...", remember_me=False)
```

```{note}
Tiled stores OAuth2 tokens (it _never_ stores your password) in files
with properly restricted permissions under `$XDG_CACHE_DIR/tiled/tokens`,
typically `~/.config/tiled/tokens` on Linux and MacOS.

To customize the location of this storage, set the environment variable
`TILED_CACHE_DIR`.
```

Some Tiled servers are configured to always require login, disallowing any
anonymous access. For those, the client will prompt immediately, such as:

   >>> from tiled.client import from_uri
   >>> client = from_uri("https://...")
   Username:
   ```

## Noninteractive Authentication (API keys)

There are environments where logging in interactively is not possible,
such as running a batch script. For these applications, we recommend
using an API key. These can be created from the CLI:

```sh
$ tiled login
$ tiled api_key create --expires-in 7d --note "for this week's experiment"
```

or from an interactive Python session:

```py
>>> client = from_uri("https://...")
>>> client.login()
>>> client.create_api_key(expires_in="7d", note="for this week's experiment")
{"secret": ...}
 ```

The expiration and note are optional, but recommended. Expiration can be given
in units of years `y`, days `d`, hours `h`, minutes `m`, or seconds `s`.

```

The best way to provide an API key is to set the environment variable
`TILED_API_KEY`. A script like this:

```py
from tiled.client import from_uri

client = from_uri("https://....")
```

will detect that `TILED_API_KEY` is set and use that API key for
authentication with Tiled. This is equivalent to:

```py
import os
from tiled.client import from_uri

client = from_uri("https://....", api_key=os.environ["TILED_API_KEY"])
```

Avoid typing the API key in to the code:

```py
from_uri("https://...", api_key="secret!")  # DON'T
```

as it is easy to accidentally share or leak.

## Custom Applications

Custom applications, such as a graphical interfaces that wrap Tiled, may not be
able to use Tiled commandline-based prompts. They should avoid using the
convenience functions `tiled.client.constructors.from_uri` and
`tiled.client.constructors.from_profile`.

They may implement their own interfaces for collecting credentials (for
password grants) or launching a browser and waiting for the user to authorize a
session (for device code grants). The functions
`tiled.client.context.password_grant` and
`tiled.client.context.device_code_grant` may be useful building blocks. The
tokens obtained from this process may then be passed directly in to the Tiled
client like so.


```py
from tiled.client import Context

URI = "https://..."
context, node_path_parts = Context.from_any_uri(URI)
tokens, remember_me = launch_custom_interface()
context.configure_auth(tokens, remember_me=remember_me)
client = from_context(context, node_path_parts=node_path_parts)
```

The client will transparently handle OAuth2 refresh flow. If the session is
revoked or expires, and an attempt at refreshing the tokens is thus rejected
by the server, the exception `tiled.client.auth.CannotRefreshAuthentication`
will be raised. The application should be prepared to catch that exception
and reinitiate authentication.
