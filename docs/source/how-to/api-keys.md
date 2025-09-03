# Create and Use API Keys

This guide applies to **multi-user** Tiled deployments, i.e. deployments configured
with an authentication provider.

```{note}

**Single-user** deployments run with a single API key. By default, it is
randomly generated and printed to the console at server startup, like so:

    $ tiled serve pyobject tiled.examples.generated:tree
    Generating large example data...
    Done generating example data.

        Use the following URL to connect to Tiled:

        "http://127.0.0.1:8000?api_key=8df0146c93e9add287d0e7f84a165ba4bd3517bbb3c7c6b4d963c3e1549d0311"

This guide does not apply to single-user deployments like this.
```

## Why use API keys?

Users can log in to Tiled with a simple username/password or through
a third party like ORICD or Google, as demonstrated in the tutorial
{doc}`../tutorials/login`. For interactive workflows in Python, this the most
convenient way to authenticate.

But in other situations, it can more convenient to generate and use an API key.
An API key uses a comparatively simpler interaction, without an interactive
prompt or the need to periodically "refresh" a session to keep it active. This
is useful when:

* The code using tiled is running in an unsupervised, automated fashion
  or scaled across many workers, such as in an HPC job, where is it not
  possible to provide credentials interactively.
* You are connecting from a generic web client like a web browser or `curl`,
  which has no built-in integration with Tiled or with OpenAPI+OAuth2.

## Create an API Key

To follow along, you may start a Tiled server with a simple authentication provider,
as shown. Alternatively, you may use an existing authenticated Tiled server, such as
`https://tiled-demo.blueskyproject.io`; if you do, replace `http://localhost:8000`
in the example below with that address.

```
# You can find example_configs/ in the root of the tiled source code repository.
ALICE_PASSWORD=secret1 tiled serve config example_configs/toy_authentication.yml
```

Note that you will need to run these helper tools to prep the backing databases that Tiled needs,
before you can use the example config shown above:
```
# prep the access tags and catalog databases
python example_configs/access_tags/compile_tags.py
python example_configs/catalog/create_catalog.py
```

Using the Tiled commandline interface, log in as `alice` using the password `secret1`.

```
$ tiled profile create http://localhost:8000
$ tiled login
Username: alice
Password:
```

Create a new API key.

```
$ tiled api_key create
48e8f8598940fa0f3e80b406def606e17e815a2c76fe21350a99d6d9935371d11533b318
```

This text is the API key. **It should be handled as a secret.**

## Use the API Key in Python

We can use it in the Python client:

```py
>>> from tiled.client import from_uri
>>> API_KEY = "YOUR_KEY_HERE"
>>> c = from_uri("http://localhost:8000", api_key=API_KEY)
```

API keys should never be placed directly in scripts or notebooks.
Instead, set the environment variable `TILED_API_KEY`.

```
export TILED_API_KEY=YOUR_KEY_HERE
```

and then start Python (or IPython, or Jupyter, or...). The Python client will
use that, unless it is explicitly passed different credentials.

```py
>>> from tiled.client import from_uri
>>> c = from_uri("http://localhost:8000")  # uses TILED_API_KEY, if set
```

## Use the API Key in other web clients

We can use in other web clients as well. For example, using [HTTPie](https://httpie.io/)
we can see that unauthenticated requests are refused

```
$ http http://localhost:8000/api/v1/metadata/
HTTP/1.1 401 Unauthorized
content-length: 30
content-type: application/json
date: Mon, 31 Jan 2022 17:30:06 GMT
server: uvicorn
server-timing: app;dur=2.6
set-cookie: tiled_csrf=bZLhKsXVE2VirgXQncHsHn4Y0Wwwr66U0T0hqarJyfw; HttpOnly; Path=/; SameSite=lax
x-tiled-root: http://localhost:8000/api/v1

{
    "detail": "Not authenticated"
}
```

but passing the API key in the `Authorization` header as `Apikey YOUR_KEY_HERE` is accepted.
(Note the use of `'` quotes.)

```
$ http http://localhost:8000/api/v1/metadata/ 'Authorization:Apikey 48e8f8598940fa0f3e80b406def606e17e815a2c76fe21350a99d6d9935371d11533b318'
HTTP/1.1 200 OK
content-length: 320
content-type: application/json
date: Mon, 31 Jan 2022 17:34:48 GMT
etag: c7d94c99b0a3cd6e102a78520db84bef
server: uvicorn
server-timing: tok;dur=0.0, pack;dur=0.0, app;dur=15.2
set-cookie: tiled_csrf=InE4mplUO0goPxf4V07tVuLSLUvDqhgtALTHYoC3T3s; HttpOnly; Path=/; SameSite=lax
<etc.>
```

The API key can also be passed in the URL like
`http://localhost:8000/api/v1/metadata/?api_key=YOUR_KEY_HERE`. Using the
`Authorization` header is preferred (more secure) but in some situations, as in
pasting a link into a web browser, the URL is the only option.

## Manage API Keys

We can use the tiled commandline interface to examine and revoke API keys as well.

```
$ tiled api_key
Usage: tiled api_key [OPTIONS] COMMAND [ARGS]...

  Create, list, and revoke API keys.

Options:
  --help  Show this message and exit.

Commands:
  create
  list
  revoke
```

We can see the key that we made above in the list.

```
$ tiled api_key list
First 8   Expires at (UTC)     Latest activity      Note                Scopes
48e8f859  -                    2022-01-31T18:32:33                      inherit

```

By default, an API key inherits all the same access
as the user who is for. If an API key will be used for a specific task, it is
good security hygiene to give it only the privileges it needs for that task.  It
is also recommended to set a limited lifetimes so that if the key is
unknowingly leaked it will not continue to work forever. For example, this
command creates an API key that will expire in 10 minutes and can
search/list metadata but cannot download array data.

```
$ tiled api_key create --expires-in 10m --scopes read:metadata
ba9af604023a829ab22edb786168d6e1b97cef68c54c6d95d7fad5e3e6347fa131263581
```

Expiration can be given in units of years `y`, days `d`, hours `h`, minutes
`m`, or seconds `s`.

See {doc}`../reference/scopes` for the full list of scopes and their capabilities.

```
$ tiled api_key list
First 8   Expires at (UTC)     Latest activity      Note                Scopes
48e8f859  -                    2022-01-31T18:32:33                      inherit
ba9af604  2022-01-31T23:03:57  -                                        read:metadata
```

Finally, the `--note` option can be used to label an API key as a reminder of
where or how it is being used.

```
$ tiled api_key create --note="Data validation pipeline" --scopes read:metadata --scopes read:data
573928c62e53096321fb874847bcc6a90ea636eebd8acbd7c5e9d18d2859847b3bfb4f19
$ tiled api_key list
First 8   Expires at (UTC)     Latest activity      Note                      Scopes
48e8f859  -                    2022-01-31T18:32:33                            inherit
ba9af604  2022-01-31T23:03:57  -                                              read:metadata
573928c6  -                    -                    Data validation pipeline  read:metadata read:data
```

If an API key is no longer need or accidentally leaked, it should be revoked. It
can be identified by its first eight characters, as shown in the output of
`tiled api_key list`.

```
$ tiled api_key revoke 573928c6
$ tiled api_key list
First 8   Expires at (UTC)     Latest activity      Note                      Scopes
48e8f859  -                    2022-01-31T18:32:33                            inherit
ba9af604  2022-01-31T23:03:57  -                                              read:metadata
```

Passing the full key also works, if you have it handy. There is no need to trim
just the first eight.
