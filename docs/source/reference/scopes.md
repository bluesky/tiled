# Scopes

Tiled uses OAuth2 scopes to restrict the actions that users, services, and API keys can perform.
See the guide {doc}`../how-to/api-keys` for instructions on generating API keys
with restricted scopes.

## List of Scopes

* `read:metadata` --- List and search metadata.
* `read:data` --- Fetch (array, table) data.
* `create` --- Create a new node.
* `write:metadata` --- Write metadata.
* `write:data` --- Write (array, table) data.
* `apikeys` --- Manage API keys for the currently-authenticated user or service.
* `metrics` --- Access Prometheus metrics.
* `admin:apikeys` --- Manage API keys on behalf of any user or service.
* `read:principals` --- Read list of all users and services and their attributes.

Finally, there is the meta-scope `inherit`, the default for API keys. It
inherits the scopes of the Principal associated with this key, resolved at
access time.

## Roles

An authenticated entity ("Principal") may be assigned roles that confer a list
of scopes.

* `user` --- default role, granted scopes `["read:metadata", "read:data", "write:metadata", "write:data", "create", "apikeys"]`
* `admin` --- granted all scopes

There is support for custom roles at the database level, but neither role
creation/customization nor role assignment are yet exposed through the API.
(This will come in a future release.)

For now, admin role can only be assigned by setting `tiled_admins` in the
service configuration, as in this example.

```{eval-rst}
.. literalinclude:: ../../../example_configs/toy_authentication.yml
   :caption: example_configs/toy_authentication.py
```
