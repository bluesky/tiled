# Access Control

Tiled offers control of which users can see which entries.
This is done using an "Access Policy" object, which implements
a given policy for a given Catalog. It can do this in one of two ways

1. Brute checking the entries one at a time
2. Using a query to narrow down search results to those that
   the authenticated user is allowed to access.

(1) is easier to implement and suitable for small- to medium-sized Catalogs
in memory or backed by a modestly-sized directory. (2) is necessary for large
Catalogs backed by databases or other external services.

This is an example of (1):

```{eval-rst}
.. literalinclude:: ../../../tiled/examples/toy_authentication.py
```


Here is an example configuration serving that catalog with a
"toy" authenticator that defines some users.

```{eval-rst}
.. literalinclude:: ../../../example_configs/toy_authentication.yml
```

If the configuration above is saved at ``config.yml``, it can be served with:

```
ALICE_PASSWORD=secret BOB_PASSWORD=secret2 CARA_PASSWORD=secret3 tiled serve config config.yml
```
