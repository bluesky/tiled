# Scopes

Tiled uses OAuth2 scopes to restrict the actions that users, services, and API keys can perform.

## Scopes

* `inherit` --- Default scope for API keys. Inherit scopes of the Principal associated with this key, resolved at access time.
* `read:metadata` --- List and search metadata.
* `read:data` --- Fetch (array, dataframe) data.
* `apikeys` --- Manage API keys for the currently-authenticated user or service.
* `metrics` --- Access Prometheus metrics.
* `admin:apikeys` --- Manage API keys on behalf of any user or service.
* `read:principals` --- Read list of all users and services and their attributes.

An authenticated entity ("Principal") may be assigned roles:

## Roles

* `user` --- default role, granted scopes `["read:metadata", "read:data", "apikeys"]`
* `admin` --- granted all scopes

There is support for custom roles at the database level, but it is not yet
exposed through the API.
