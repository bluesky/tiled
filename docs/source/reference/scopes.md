# Scopes

Tiled uses OAuth2 scopes to restrict the actions that users, services, and API keys can perform.
See the guide {doc}`../how-to/api-keys` for instructions on generating API keys
with restricted scopes.

## List of Scopes

* `inherit` --- Default scope for API keys. Inherit scopes of the Principal
  associated with this key, resolved at access time.
* `read:metadata` --- List and search metadata.
* `read:data` --- Fetch (array, dataframe) data.
* `write:metadata` --- Write metadata. This is not yet used by Tiled itself. It is made available for use by experimental externally-developed adapters that support writing.
* `write:data` --- Write (array, dataframe) data. This is not yet used by Tiled itself. It is made available for use by experimental externally-developed adapters that support writing.
* `apikeys` --- Manage API keys for the currently-authenticated user or service.
* `metrics` --- Access Prometheus metrics.
* `admin:apikeys` --- Manage API keys on behalf of any user or service.
* `read:principals` --- Read list of all users and services and their attributes.

## Roles

An authenticated entity ("Principal") may be assigned roles that confer a list
of scopes.

* `user` --- default role, granted scopes `["read:metadata", "read:data", "apikeys"]`
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
