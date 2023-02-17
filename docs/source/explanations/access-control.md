# Access Control

Tiled uses a programmable Access Control Policy to manage which
users can access and perform actions on (read, write, delete) which entries.

For use background on how to think about access control generally, we
recommend the talk
[Why Authorization Is Hard](https://www.youtube.com/watch?v=2BN96ON48U8),
(Sam Scott, PyCon 2022).

A key point is that if a user cannot access something, it should be as if it
does not exist. In a user interface built on Tiled, whether graphical or
terminal-based, we don't want to even _show_ the user buttons or options that
they are not allowed to use.

In support of this, a Tiled Access Control Policy answers two questions:

1. Given one specific dataset, what is the user allowed to do with it (read,
   write, etc.)?
2. Given a hierarchical dataset, which of the "children" is the user allowed to
   {read, read and write, ...}?

The first one lets us efficiently ask which "buttons" to show for a given data
set. The second one lets us efficiently ask which items to show (or hide...) in
a list of contents.

Rephrasing these two items now using the jargon of entities in Tiled:

1. Given a Principal (user or service) and a Node, return a list of the scopes
   (actions) the Principal is allowed to perform on that Node.

2. Given a Principal (user or service), a Node, and a list of scopes (actions),
   return a list of Query objects that, when applied to the Node, filters its
   children such that the Principal can do all of those actions on the remaining
   children (if any).

This determination can be backed by a call to an external service or by a
static configuration file. We demonstrate both here.

First, the static configuration file. Consider this simple tree of data:

```{eval-rst}
.. literalinclude:: ../../../tiled/examples/toy_authentication.py
   :caption: tiled/examples/toy_authentication.py
```

protected by this simple Access Control Policy:

```{eval-rst}
.. literalinclude:: ../../../example_configs/toy_authentication.yml
   :caption: example_configs/toy_authentication.yml
```

Under `access_lists:` usernames are mapped to the keys of the entries the user may access.
The section `public:` designates entries that an
unauthenticated (anonymous) user may access *if* the server is configured to
allow anonymous access. (See {doc}`security`.) The special value
``tiled.adapters.mapping:SimpleAccessPolicy.ALL`` designates that the user may access any entry
in the Tree.

```
ALICE_PASSWORD=secret1 BOB_PASSWORD=secret2 CARA_PASSWORD=secret3 tiled serve config example_configs/config.yml
```

For large-scale deployment, Tiled typically integrates with an existing access management
system. This is sketch of the Access Control Policy used by NSLS-II to
integrate with our proposal system.

```py
import cachetools
import httpx
from tiled.queries import In


# To reduce load on the external service and to expedite repeated lookups, use a
# process-global cache with a timeout.
response_cache = cachetools.TTLCache(maxsize=10_000, ttl=60)


class PASSAccessPolicy:
    """
    access_control:
      access_policy: pass_access_policy:PASSAccessPolicy
      args:
        url: ...
        beamline: ...
    """

    def __init__(self, url, beamline, provider):
        self._client = httpx.Client(base_url=url)
        self._beamline = beamline
        self.provider = provider

    def _get_id(self, principal):
        for identity in principal.identities:
            if identity.provider == self.provider:
                return identity.id
        else:
            raise ValueError(
                f"Principcal {principal} has no identity from provider {self.provider}. "
                f"Its identities are: {principal.identities}"
            )

    def allowed_scopes(self, node, principal):
        return {"read:metadata", "read:data"}

    def filters(self, node, principal, scopes):
        queries = []
        id = self._get_id(principal)
        if not scopes.issubset({"read:metadata", "read:data"}):
            return NO_ACCESS
        try:
            response = response_cache[id]
        except KeyError:
            response = self._client.get(f"/data_session/{id}")
            response_cache[id] = response
        if response.is_error:
            response.raise_for_status()
        data = response.json()
        if ("nsls2" in (data["facility_all_access"] or [])) or (
            self._beamline in (data["beamline_all_access"] or [])
        ):
            return queries
        queries.append(
            In("data_session", data["data_sessions"] or [])
        )
        return queries
```
