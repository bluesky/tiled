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

The seeded graph is shown below. The six `linked` datasets (bound to catalog
nodes) form a reduction pipeline joined by `prov:wasDerivedFrom`; the raw
`measured` stack also reaches outward to a calibration image on another Tiled
server and to the sample's encyclopedic record. An `analysis_workflow` (run by
`processing_script.py`) and an RO-Crate manifest tie the same datasets together
from two other perspectives.

```{mermaid}
flowchart TD
    subgraph linked["linked container (catalog nodes)"]
        measured["measured<br/>(dataset)"]
        background["background<br/>(dataset)"]
        subtracted["subtracted<br/>(dataset)"]
        normalized["normalized<br/>(dataset)"]
        integrated["integrated<br/>(dataset)"]
        summary["summary<br/>(dataset)"]
    end

    subgraph ext["external entities"]
        beam["dif_beam_hdf5_image<br/>(dataset, another Tiled server)"]
        sample["lanthanum_hexaboride<br/>(reference, Wikidata)"]
    end

    workflow["analysis_workflow<br/>(workflow)"]
    script["processing_script.py<br/>(software)"]
    crate["ro-crate-metadata.json<br/>(rocrate)"]

    subtracted -->|prov:wasDerivedFrom| measured
    subtracted -->|prov:wasDerivedFrom| background
    normalized -->|prov:wasDerivedFrom| subtracted
    integrated -->|prov:wasDerivedFrom| normalized
    summary -->|prov:wasDerivedFrom| normalized

    measured -->|prov:wasInformedBy| beam
    measured -->|schema:about| sample

    workflow -->|instrument| script
    workflow -->|prov:used| measured
    workflow -->|prov:used| background
    workflow -->|result| subtracted
    workflow -->|result| normalized
    workflow -->|result| integrated
    workflow -->|result| summary

    crate -->|ro:hasPart| measured
    crate -->|ro:hasPart| background
    crate -->|ro:hasPart| subtracted
    crate -->|ro:hasPart| normalized
    crate -->|ro:hasPart| integrated
    crate -->|ro:hasPart| summary
```

## Using the Python client

If you are working from a Tiled Python client, you can build the graph entirely
in Python, without writing any GraphQL code.

The usual node clients---what you get from `from_uri(...)` or `client["key"]`---
point at datasets and containers in the catalog tree and are your primary
interface to the data. The graph adds two lightweight companions: an
`EntityHandle` represents a subject or an object in a directed relationship, and
that relationship itself is represented by a `LinkHandle`. Both are only
references to graph records---they hold no array or table data, encoding instead
the relationships between Tiled nodes. An entity is usually bound to a catalog
node, but it can also point to an external resource.

You rarely need to construct these handles yourself. Every node client exposes
`bind_entity()` and `entities()` methods that return them, and a few extra
helpers in `tiled.client.graph` link entities and create ones that point outside
the catalog.

The simplest place to start is to bind an entity to a dataset you already have a
client for. By default the entity takes its `kind` (the entity type) from the
node's structure family---`"array"`, `"table"`, `"container"`, and so on---and
its `name` from the node's key, but you can override either one:

```python
from tiled.client import from_uri
from tiled.client.graph import make_entity, make_link, register_namespace

client = from_uri("http://127.0.0.1:8000", api_key="secret")

client.write_array([1, 2, 3], key="measured")
client.write_array([0, 1, 2], key="subtracted")

# Register a CURIE prefix so predicates like "prov:wasDerivedFrom" resolve.
register_namespace(client, "prov", "http://www.w3.org/ns/prov#")

measured = client["measured"].bind_entity()                 # kind="array", name="measured"
subtracted = client["subtracted"].bind_entity(kind="derived")
```

Because each `(node, kind, name)` combination must be unique, `bind_entity()`
raises `EntityExistsError` when a matching entity already exists. In that case,
use the node's `entities()` method to fetch what is already there:

```python
existing = client["measured"].entities()  # list of EntityHandle
```

Not every entity corresponds to a node in the catalog. To describe something
that lives elsewhere---a physical sample, an instrument, or a dataset on another
server---create a free-standing entity with `make_entity()`:

```python
sample = make_entity(
    client,
    kind="sample",
    name="LaB6",
    uri="http://www.wikidata.org/entity/Q0",
)
```

Finally, connect any two entities with a link. Links are directed,
reading as `subject -predicate-> object`, and `subject` and `object` are
keyword-only so that direction is always unambiguous:

```python
make_link(subject=subtracted, object=measured, predicate="prov:wasDerivedFrom")
make_link(subject=measured, object=sample, predicate="prov:used")
```

### Explore and update the graph

Once entities and links exist, you can look them up, walk the connections
between them, and revise or remove them---all through the same handles. Every
lookup returns an `EntityHandle` or `LinkHandle` (or `None` when nothing
matches), so results can be chained straight into further calls.

The `GraphClient` is the entry point for id lookups and filtered searches:

```python
from tiled.client.graph import GraphClient

graph = GraphClient(client.context)

graph.get_entity(measured.id)               # one entity by id, or None
graph.find_entities(kind="derived")         # filter by kind, name, and/or node
graph.find_links(subject_id=subtracted.id)  # filter by subject, predicate, object
```

From an entity handle you can traverse its links in either direction, and from
a link handle you can resolve the entities at each end:

```python
outgoing = subtracted.outgoing_links()   # links where this entity is the subject
incoming = measured.incoming_links()     # links where this entity is the object

link = outgoing[0]
link.subject()   # the EntityHandle at the tail (== subtracted)
link.object()    # the EntityHandle at the head (== measured)
```

Handles can be edited in place. `update()` sends only the fields you pass and
mutates the handle to match the server's response:

```python
sample.update(name="LaB6 (NIST SRM 660c)")   # rename an entity
sample.update(uri=None)                       # clear a field by passing None
link.update(predicate="prov:wasRevisionOf")   # relabel a link

sample.delete()   # remove an entity (and every link attached to it)
link.delete()     # remove a single link
```

When editing an entity bound to a catalog node on this Tiled server, pass
`node_path_parts=None` to detach it from its catalog node, or a new path to re-bind
it; omit the argument to leave the binding untouched.

All Python helpers described above wrap the same GraphQL API covered in the next
section.

## Using GraphQL directly

The Python helpers above are a thin convenience layer over the GraphQL API,
which you can also drive by hand---from the in-browser editor, from `curl`, or
from any HTTP client. The sections below walk through querying and mutating the
graph directly.

### Open the GraphiQL editor

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

### Query entities and links

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

### Trace provenance across the graph

So far, each query has returned a flat list of entities or links. But entities
and links form a connected graph, and GraphQL lets you walk it. From an entity,
`outgoingLinks` follows the edges leaving it (and `incomingLinks` the edges
arriving); each link's `object` is another entity, with its own links. The following
example demonstrates how to nest these fields and a single query to hop from entity
to entity, tracing a chain across the graph.

The demo connects its datasets into a reduction pipeline with
`prov:wasDerivedFrom` edges (`integrated` was derived from `normalized`, which
was derived from `subtracted`, which was derived from `measured` and
`background`). Starting from the `integrated` result, one query can reconstruct
the whole lineage---and, at the raw `measured` dataset, reach out to its
external context (a calibration image on *another* Tiled server and the
sample's encyclopedic record).

First get the id of the `integrated` entity (from the `entities` query above,
or filter by name in your client), then run:

```graphql
query Lineage($id: ID!) {
  entity(id: $id) {
    name
    outgoingLinks(predicate: "prov:wasDerivedFrom") {
      object {
        name
        outgoingLinks(predicate: "prov:wasDerivedFrom") {
          object {
            name
            outgoingLinks(predicate: "prov:wasDerivedFrom") {
              object {
                name
                kind
                outgoingLinks {
                  predicate
                  object { name kind uri }
                }
              }
            }
          }
        }
      }
    }
  }
}
```

with the id supplied in the **Variables** pane:

```json
{
  "id": "<id of integrated>"
}
```

The response reconstructs the entire provenance in one round trip:

```
integrated
 └─ normalized
     └─ subtracted
         ├─ measured   (prov:wasInformedBy → dif_beam_hdf5_image, on another Tiled server)
         │             (schema:about       → lanthanum_hexaboride, on Wikidata)
         └─ background
```

The `predicate:` argument filters each hop to just the lineage edges; drop it
(as the innermost `outgoingLinks` does) to see *all* edges leaving a node. To
ask the mirror-image question---"what was derived *from* `measured`?"---start
from `measured` and follow `incomingLinks(predicate: "prov:wasDerivedFrom")`
instead.

```{note}
The graph is recursively traversable, so a query could nest
`outgoingLinks` arbitrarily deep and become expensive to resolve. Tiled caps
nesting at a fixed depth (10 levels); a query deeper than that is rejected.
Introspection queries (the GraphiQL **Docs** panel) are exempt.
```

### Query namespaces

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

#### See namespaces together with the data

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

### Create entities and links

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

### Tie entities to data: `nodePathParts` vs `uri`

An entity can reference the data it describes in two independent ways:

- **`nodePathParts`** --- the path of key segments to a node in *this* server's
  own catalog tree, for an entity that represents (or is closely tied to) a
  dataset hosted right here. Pass it directly to `createEntity`
  (`["linked", "measured"]` for the measured dataset in the demo, `["a", "b"]`
  for any nested entry); the server resolves it to the node's internal id, which
  is never exposed to clients. The read-only `isNodeBound` field on an entity
  reports whether it is tied to a node, and the query and mutation both raise an
  error if `nodePathParts` names no existing node.

- **`uri`** --- a free-form locator, stored and returned verbatim with no
  lookup or validation. Follow this convention when setting it:
  - If the entity points at data hosted by *this* server, set `uri` to the
    full Tiled URL alongside `nodePathParts` (e.g.
    `http://host:port/api/v1/metadata/linked/measured`), so the entity is
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
      "name": "measured",
      "nodePathParts": ["linked", "measured"],
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
      "kind": "dataset",
      "name": "dif_beam_hdf5_image",
      "uri": "https://tiled-demo.nsls2.bnl.gov/api/v1/metadata/csx/6cb250e3-3a4a-46e1-8fcb-a1caa0445f41/primary/dif_beam_hdf5_image",
      "properties": { "@type": "Dataset" }
    }
  }
  ```

  This entity is not node-bound (`isNodeBound` is false)---it isn't in this
  server's catalog---but it can still be linked into the graph like any other
  entity. In the demo it is the object of a `prov:wasInformedBy` link from the
  local `measured` dataset, recording that the measurement was calibrated
  against an image that lives on a *different* Tiled deployment. This is how a
  graph can span multiple Tiled servers: the entity is a lightweight,
  `uri`-addressed stand-in for remote data, so provenance crosses deployment
  boundaries without copying anything.

  The same pattern works for any stable external identifier, not just Tiled
  URLs. The demo also includes a `lanthanum_hexaboride` entity whose `uri` is
  a [Wikidata](https://www.wikidata.org/wiki/Q410318) IRI, linked from
  `measured` with `schema:about` to tie the measurement to an open,
  encyclopedic record of the sample material. Other good choices for such
  references include a DOI, a [DBpedia](https://www.dbpedia.org/) resource, or
  a domain database entry (e.g. a PDB or Crystallography Open Database id).

  See the `dif_beam_hdf5_image` and `lanthanum_hexaboride` entities in
  `tiled/examples/demo_graph.json` for worked examples.

### From the command line

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
