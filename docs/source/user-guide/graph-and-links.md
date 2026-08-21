The Graph of Links feature is experimental. The APIs may change.

# Explore the Entity/Link Graph with GraphQL

Tiled can optionally serve a graph of entities connected by links, alongside a
catalog-backed tree, queryable through a GraphQL API. See
{doc}`../explanations/graphs` for background on what this feature is and why
it exists.

This guide walks you through the process of starting a demo server with the graph enabled and
exploring it interactively in the browser.

## Enable the graph feature

The graph is available automatically whenever a server is backed by a SQL-based
catalog tree (see {doc}`example-server-config`) -- there is no separate
configuration flag. The built-in demo server is the easiest way to try it:

```
tiled serve demo
```

This starts a public server, populates a catalog of datasets (a `linked`
container holding a `measured` image stack and a `background` frame, reduced
into `subtracted`, `normalized`, and `integrated` datasets plus a tabular
`summary`, alongside a broader showcase of data structures), and seeds a small
provenance graph connecting them---using
[PROV](https://www.w3.org/TR/prov-o/) and [RO](https://w3id.org/ro/terms/)
predicates, and linking out to data on another Tiled server and to an
encyclopedic reference for the sample---then leaves the server running.

```{note}
The demo is public: anonymous access is read-only, so you can browse the graph
without credentials. *Writing* data or mutating the graph requires the
single-user API key (default `secret`), which is printed at startup.
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

The read-only queries below run without credentials. To *create* or *modify*
entities and links, send the API key as an `Authorization` header. In GraphiQL
there is a small tab row at the bottom of the query-editing pane, usually
labeled **Variables** / **Headers** (sometimes collapsed behind a settings
icon). Click **Headers** and enter:

```json
{
  "Authorization": "Apikey secret"
}
```

On a server that is *not* public, read queries also require this header;
without it they will not raise an error---they will simply return empty
results, because Tiled's access checks fail closed.

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
    entityType
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
    entityType
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
    "entityType": "dataset",
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

## Tie entities to data: `nodeId` vs `uri`

An entity can reference the data it describes in two independent ways:

- **`nodeId`** --- the internal id of a node in *this* server's own catalog
  tree, for an entity that represents (or is closely tied to) a dataset
  hosted right here. Catalog node ids are internal and are not otherwise
  exposed to clients, so resolve one from a catalog path with the
  `catalogNodeId` query before creating or updating an entity:

  ```graphql
  query {
    catalogNodeId(path: ["linked", "measured"])
  }
  ```

  `path` is the list of key segments leading to the node (`["linked", "measured"]`
  for the measured dataset in the demo, `["a", "b"]` for any nested entry). It
  returns `null` if no such node exists.

- **`uri`** --- a free-form locator, stored and returned verbatim with no
  lookup or validation. Follow this convention when setting it:
  - If the entity points at data hosted by *this* server, set `uri` to the
    full Tiled URL alongside `nodeId` (e.g.
    `http://host:port/api/v1/metadata/linked/measured`), so the entity is
    resolvable both internally (via `nodeId`) and as a plain link (via
    `uri`).
  - If it points at data hosted elsewhere (a dataset on a different Tiled
    deployment, a DOI, anything with a stable external address), set `uri`
    to that address and leave `nodeId` unset.
  - If it doesn't point at any addressable resource (e.g. a workflow or
    software entity that only exists as a description), leave `uri` unset
    (`null`).

  An entity tied to local data, with both fields set (again, `properties`
  goes in the Variables pane):

  ```graphql
  mutation CreateEntity($input: CreateEntityInput!) {
    createEntity(input: $input) {
      id
      nodeId
      uri
    }
  }
  ```

  ```json
  {
    "input": {
      "entityType": "dataset",
      "name": "measured",
      "nodeId": 1,
      "uri": "http://127.0.0.1:8000/api/v1/metadata/linked/measured",
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
      "entityType": "dataset",
      "name": "dif_beam_hdf5_image",
      "uri": "https://tiled-demo.nsls2.bnl.gov/api/v1/metadata/csx/6cb250e3-3a4a-46e1-8fcb-a1caa0445f41/primary/dif_beam_hdf5_image",
      "properties": { "@type": "Dataset" }
    }
  }
  ```

  This entity has no `nodeId`---it isn't in this server's catalog---but it
  can still be linked into the graph like any other entity. In the demo it is
  the object of a `prov:wasInformedBy` link from the local `measured` dataset,
  recording that the measurement was calibrated against an image that lives on
  a *different* Tiled deployment. This is how a graph can span multiple Tiled
  servers: the entity is a lightweight, `uri`-addressed stand-in for remote
  data, so provenance crosses deployment boundaries without copying anything.

  The same pattern works for any stable external identifier, not just Tiled
  URLs. The demo also includes a `lanthanum_hexaboride` entity whose `uri` is
  a [Wikidata](https://www.wikidata.org/wiki/Q410318) IRI, linked from
  `measured` with `schema:about` to tie the measurement to an open,
  encyclopedic record of the sample material. Other good choices for such
  references include a DOI, a [DBpedia](https://www.dbpedia.org/) resource, or
  a domain database entry (e.g. a PDB or Crystallography Open Database id).

  See the `dif_beam_hdf5_image` and `lanthanum_hexaboride` entities in
  `tiled/examples/demo_graph.json` for worked examples.

## From the command line

The same endpoint works with any HTTP client:

```
curl -s http://127.0.0.1:8000/api/graphql \
  -H "Authorization: Apikey secret" \
  -H "Content-Type: application/json" \
  -d '{"query": "query { links { id predicate } entities { id name } }"}'
```

The graph the demo builds is defined in `tiled/examples/demo_graph.json`, a
single JSON-LD-style document listing the entities and links. The code that
reads it and creates them through GraphQL---registering namespaces, resolving
catalog node ids, and resolving human-readable entity names to their generated
ids---lives in `tiled/examples/demo.py` (`seed_graph`). Edit `demo_graph.json`
to add your own entities and links to the demo.
