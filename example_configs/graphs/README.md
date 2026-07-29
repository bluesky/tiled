# Startup Graphs Demo

This example adds a small graph workflow on top of a local Tiled catalog:

1. Start a Tiled server from `graph_example_config.yml`
2. Create one raw dataset and three derived datasets in the catalog, each with a stable `tiled_uid`
3. Read `input.json` and create graph entities/links through GraphQL

The graph now includes a richer RO-Crate-style relationship set, including:

- `derived_dataset_* -[prov:wasDerivedFrom]-> raw_dataset`
- `ro-crate-metadata.json -[hasPart]-> datasets`
- `analysis_workflow -[instrument]-> processing_script.py`
- `analysis_workflow -[result]-> derived datasets`

## Run

From the repository root:

```bash
bash example_configs/graphs/run_demo.sh
```

The script seeds data, creates links, and then keeps the server running until
you press Ctrl+C. If port 8000 is already in use, it automatically selects
the next free port and starts the server there instead.

The input graph definition is:

- `example_configs/graphs/input.json`

Edit `input.json` to add your own entities and links. Dataset names in that
file should match the dataset keys defined in `create_datasets.py`.

## Files

- `graph_example_config.yml`: server config with a local catalog database
- `create_datasets.py`: seeds catalog datasets with stable `tiled_uid` metadata
- `input.json`: editable source used by the graph script
- `create_links.py`: reads `input.json` and creates graph entities/links with GraphQL
- `run_demo.sh`: orchestration script
- `serve_with_config.py`: starts server from `graph_example_config.yml` for this demo

## Explore via GraphQL

Once the server is running (default `http://127.0.0.1:8000`), you can browse
the graph interactively at the GraphiQL IDE:

```
http://127.0.0.1:8000/api/graphql
```

`graph_example_config.yml` sets `allow_anonymous_access: false`, so every
request needs an API key. In GraphiQL, open the **Headers** tab at the bottom
of the query editor pane (separate from **Variables**) and add:

```json
{
  "Authorization": "Apikey secret"
}
```

Without this header, queries won't error — they'll just silently return empty
results, since access checks fail closed.

### List all links

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
`predicate` filter can be given as a CURIE (e.g. `"schema:relatedTo"`) if that
prefix is registered — see below.

### List registered namespaces

```graphql
query {
  namespaces {
    prefix
    uri
  }
}
```

Namespaces are CURIE prefix -> URI mappings (e.g. `schema -> https://schema.org/`)
used to expand/compact property keys and link predicates. They only show up
here if something registered them, via an explicit mutation:

```graphql
mutation {
  upsertNamespace(prefix: "schema", uri: "https://schema.org/") {
    prefix
    uri
  }
}
```

### Namespaces together with the data

Because GraphQL lets you request multiple top-level fields in one query, you
can see everything resolved consistently in a single round trip:

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
against the `namespaces` list — a property stored as the full IRI
`https://schema.org/name` displays here as `schema:name` if the `schema`
prefix is registered.

### From the command line

```bash
curl -s http://127.0.0.1:8000/api/graphql \
  -H "Authorization: Apikey secret" \
  -H "Content-Type: application/json" \
  -d '{"query": "query { links { id predicate } entities { id name } }"}'
```
