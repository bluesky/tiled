# Access Control

Tiled offers extensible access control over which users can access
which entries.

This is implemented using an "Access Policy" object, which implements
a given policy for certain types of Tree. It can do this in one of two ways:

1. Brute-force checking the entries one at a time
2. Using a query to narrow down search results to those that
   the authenticated user is allowed to access.

(1) is easier to implement and suitable for small- to medium-sized Trees
in memory or backed by a modestly-sized directory. (2) is necessary for large
Trees backed by databases or other external services.

This is an example of (1):

```{eval-rst}
.. literalinclude:: ../../../tiled/examples/toy_authentication.py
   :caption: tiled/examples/toy_authentication.py
```

```{eval-rst}
.. literalinclude:: ../../../example_configs/toy_authentication.yml
   :caption: example_configs/toy_authentication.py
```

Under `access_lists:` usernames are mapped to the keys of the entries the user may access.
The section `public:` designates entries that an
unauthenticated (anonymous) user may access *if* the server is configured to
allow anonymous access. (See {doc}`security`.) The special value
``tiled.trees.in_memory:SimpleAccessPolicy.ALL`` designates that the user may access any entry
in the Tree.

```
ALICE_PASSWORD=secret1 BOB_PASSWORD=secret2 CARA_PASSWORD=secret3 tiled serve config example_configs/config.yml
```

Implementing (2) is highly situation dependent. Here is a sketch of the Access Policy
used by NSLS-II to integrate with our proposal system and MongoDB database of metadata.

```py
import cachetools
from databroker.mongo_normalized import Tree
import httpx

# Use a process-global cache that instances of PASSAccessPolicy share.
response_cache = cachetools.TTLCache(maxsize=10_000, ttl=60)

if __debug__:
    import logging
    import os

    logger = logging.getLogger(__name__)
    handler = logging.StreamHandler()
    handler.setLevel("DEBUG")
    handler.setFormatter(logging.Formatter("PASS ACCESS POLICY: %(message)s"))
    logger.addHandler(handler)

    log_level = os.getenv("PASS_ACCESS_POLICY_LOG_LEVEL")
    if log_level:
        logger.setLevel(log_level.upper())

class PASSAccessPolicy:
    """
    access_control:
      access_policy: pass_access_policy:PASSAccessPolicy
      args:
        url: ...
        beamline: ...
    """

    def __init__(self, url, beamline):
        self._client = httpx.Client(base_url=url)
        self._beamline = beamline

    def check_compatibility(self, catalog):
        return isinstance(catalog, Tree)

    def modify_queries(self, queries, authenticated_identity):
        try:
            response = response_cache[authenticated_identity]
        except KeyError:
            logger.debug("%s: Cache miss", authenticated_identity)
            response = self._client.get(f"/data_session/{authenticated_identity}")
            response_cache[authenticated_identity] = response
        else:
            logger.debug("%s: Cache hit", authenticated_identity)
        if response.status_code != 200:
            # TODO Fast-path for access policy to say "no access"
            modified_queries = list(queries)
            modified_queries.append({"data_session": {"$in": []}})
            try:
                response.raise_for_status()
            except Exception:
                logger.exception("%s: Failure", authenticated_identity)
            return modified_queries
        data = response.json()
        if ("nsls2" in (data["facility_all_access"] or [])) or (
            self._beamline in (data["beamline_all_access"] or [])
        ):
            logger.debug("%s: all access", authenticated_identity)
            return queries
        modified_queries = list(queries)
        modified_queries.append(
            {"data_session": {"$in": (data["data_sessions"] or [])}}
        )
        logger.debug("%s: access to %d data sessions", authenticated_identity, len(data["data_sessions"]))
        return modified_queries
```
