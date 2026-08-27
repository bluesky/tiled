The Graph of Links feature is experimental. The APIs may change.

# Explore the Entity/Link Graph with GraphQL

Tiled can optionally serve a graph of entities connected by links, alongside a
catalog-backed tree, queryable through a GraphQL API. See
{doc}`../explanations/graphs` for background on what this feature is and why
it exists.

This guide walks you through the process of starting a demo server with the graph enabled and
exploring it interactively in the browser.

## Enable the graph feature

The graph is available automatically whenever a server is serving a
catalog-backed tree (see {doc}`example-server-config`)---there is no separate
configuration flag. A ready-to-run demo can be found in
`example_configs/graphs/` in the Tiled source repository:

```
bash example_configs/graphs/run_demo.sh
```

This starts a server, seeds a small catalog of datasets, creates a handful of
graph entities and links between them (using [PROV](https://www.w3.org/TR/prov-o/)
and [RO](https://w3id.org/ro/terms/) predicates), then leaves the server
running.

```{note}
The demo config (`example_configs/graphs/graph_example_config.yml`) disables
anonymous access and uses the single-user API key `secret`. Every request,
including from the GraphiQL editor below, needs an
`Authorization: Apikey secret` header.
```

## Open the GraphQL editor

With the server running (default `http://127.0.0.1:8000`), open this URL in a
browser:

```
http://127.0.0.1:8000/api/graphql
```

This serves [GraphiQL](https://github.com/graphql/graphiql), an interactive
in-browser editor for GraphQL. The same URL also accepts `POST` requests
programmatically, from `curl` or any HTTP client (see "From the command
line" below).

Before running any query, add the API key. In GraphiQL there is a small tab
row at the bottom of the query-editing pane, usually labeled **Variables** /
**Headers** (sometimes collapsed behind a settings icon). Click **Headers**
and enter:

```json
{
  "Authorization": "Apikey secret"
}
```

Without this header, queries will not raise an error---they will simply return
empty results, because Tiled's access checks fail closed.

## Query entities and links

List all links, including the entities at each end:

```graphql
query {
  links(limit: 100, offset: 0) {
    id
    subjectId
    predicate
    objectId
    properties
    accessBlob
    createdAt
    subject {
      id
      name
    }
    object {
      id
      name
    }
  }
}
```

`links` also accepts `subjectId`, `predicate`, and `objectId` filters. A
`predicate` filter may be given as a CURIE, such as `"prov:wasDerivedFrom"`,
if that prefix is registered as a namespace (see below)---it is expanded to
its full IRI before matching.

List all entities:

```graphql
query {
  entities(limit: 100, offset: 0) {
    id
    name
    kind
    uri
    properties
  }
}
```

## Query namespaces

Namespaces are the CURIE prefix -> URI mappings used to expand and compact
property keys and link predicates (for example, `prov` ->
`http://www.w3.org/ns/prov#`). List the namespaces currently registered:

```graphql
query {
  namespaces {
    prefix
    uri
  }
}
```

Namespaces only appear here if something registered them, via an explicit
mutation:

```graphql
mutation {
  upsertNamespace(prefix: "schema", uri: "https://schema.org/") {
    prefix
    uri
  }
}
```

`deleteNamespace(prefix: "schema")` removes one. Both mutations require
`write:metadata` scope.

### See namespaces together with the data

GraphQL lets you ask for multiple top-level fields in one query, so you can
see everything resolved consistently in a single round trip:

```graphql
query {
  namespaces {
    prefix
    uri
  }
  entities {
    id
    name
    kind
    properties
  }
  links {
    id
    predicate
    subjectId
    objectId
    properties
  }
}
```

`entities[].properties` and `links[].predicate` are automatically compacted
against the `namespaces` list---a property stored internally as the full IRI
`http://www.w3.org/ns/prov#wasDerivedFrom` displays here as
`prov:wasDerivedFrom` if the `prov` prefix is registered.

## Create entities and links

Mutations require `write:metadata` scope (the demo's single-user API key has
it). Create an entity. Because `properties` is a free-form JSON scalar, pass
it through the query's **Variables** pane (the tab mentioned above,
alongside **Headers**) rather than writing it inline---an object key like
`"schema:encodingFormat"` is not valid GraphQL syntax in a literal:

```graphql
mutation CreateEntity($input: CreateEntityInput!) {
  createEntity(input: $input) {
    id
    name
    properties
  }
}
```

```json
{
  "input": {
    "kind": "dataset",
    "name": "my_dataset",
  }
}
```

Then link it to another entity by `id`:

```graphql
mutation {
  createLink(
    input: {
      subjectId: "<id of my_dataset>"
      predicate: "prov:wasDerivedFrom"
      objectId: "<id of another entity>"
    }
  ) {
    id
    predicate
  }
}
```

Property keys and predicates written this way are expanded against the
namespace registry---register the `prov` or `schema` namespace first (as
shown above) if you want the CURIE to resolve to something meaningful rather
than being stored as a literal string.

`updateEntity`, `deleteEntity`, `updateLink`, and `deleteLink` are also
available; deleting an entity cascades to any links attached to it.

## Tie entities to data: `nodePathParts` vs `uri`

An entity can reference the data it describes in two independent ways:

- **`nodePathParts`** --- the path of key segments to a node in *this* server's
  own catalog tree, for an entity that represents (or is closely tied to) a
  dataset hosted right here. Pass it directly to `createEntity` (`["raw_dataset"]`
  for a top-level entry, `["a", "b"]` for a nested one); the server resolves it to
  the node's internal id, which is never exposed to clients. The read-only
  `isNodeBound` field on an entity reports whether it is tied to a node, and the
  query and mutation both raise an error if `nodePathParts` names no existing
  node.

- **`uri`** --- a free-form locator, stored and returned verbatim with no
  lookup or validation. Follow this convention when setting it:
  - If the entity points at data hosted by *this* server, set `uri` to the
    full Tiled URL alongside `nodePathParts` (e.g.
    `http://host:port/api/v1/metadata/raw_dataset`), so the entity is
    resolvable both internally (via the bound node) and as a plain link (via
    `uri`).
  - If it points at data hosted elsewhere (a dataset on a different Tiled
    deployment, a DOI, anything with a stable external address), set `uri`
    to that address and leave `nodePathParts` unset.
  - If it doesn't point at any addressable resource (e.g. a workflow or
    software entity that only exists as a description), leave `uri` unset
    (`null`).

  An entity tied to local data, with both fields set (again, `properties`
  goes in the Variables pane):

  ```graphql
  mutation CreateEntity($input: CreateEntityInput!) {
    createEntity(input: $input) {
      id
      isNodeBound
      uri
    }
  }
  ```

  ```json
  {
    "input": {
      "kind": "dataset",
      "name": "raw_dataset",
      "nodePathParts": ["raw_dataset"],
      "uri": "http://127.0.0.1:8000/api/v1/metadata/raw_dataset",
      "properties": { "schema:encodingFormat": "application/x-zarr" }
    }
  }
  ```

  An entity referencing a dataset hosted on a different Tiled server, with
  only `uri` set:

  ```graphql
  mutation CreateEntity($input: CreateEntityInput!) {
    createEntity(input: $input) {
      id
      uri
    }
  }
  ```

  ```json
  {
    "input": {
      "kind": "dataset",
      "name": "dif_beam_hdf5_image",
      "uri": "https://tiled-demo.nsls2.bnl.gov/api/v1/metadata/csx/6cb250e3-3a4a-46e1-8fcb-a1caa0445f41/primary/dif_beam_hdf5_image",
      "properties": { "@type": "Dataset" }
    }
  }
  ```

  This entity is not node-bound (`isNodeBound` is false)---it isn't in this
  server's catalog---but it can still be linked into the graph like any other
  entity, for example as the object of a `prov:wasDerivedFrom` link from a local
  dataset, to record that the local data was derived from an experiment run
  somewhere else.

  See the `dif_beam_hdf5_image` entity in `example_configs/graphs/input.json`
  for a worked example.

## From the command line

The same endpoint works with any HTTP client:

```
curl -s http://127.0.0.1:8000/api/graphql \
  -H "Authorization: Apikey secret" \
  -H "Content-Type: application/json" \
  -d '{"query": "query { links { id predicate } entities { id name } }"}'
```

See `example_configs/graphs/input.json` and
`example_configs/graphs/create_links.py` for a complete, runnable example of
creating entities and links through GraphQL, including registering
namespaces and resolving human-readable entity names to their generated ids.
