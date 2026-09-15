# Entity/Link Graph Demo

The entity/link graph feature does not require any additional configuration and
is enabled by default. It is demonstrated by the built-in demo server, which serves
a catalog of datasets together with a small provenance graph connecting them:

```bash
tiled serve demo
```

Then open the GraphiQL editor at <http://127.0.0.1:8000/api/graphql> and explore
the entities and links. See the
[Graph of Links user guide](../../docs/source/user-guide/graph-and-links.md)
for a full walkthrough.

## Editing the graph

The graph's entities and links are defined in a single JSON-LD-style document
that ships inside the package:

```
tiled/examples/demo_graph.json
```

Edit that file (or copy it and point your own tooling at it) to change the
datasets, entities, links, predicates, and namespaces the demo creates. Dataset
entity names in that file must match the dataset keys written by the demo (see
`tiled/examples/demo.py`).

The demo is public (anonymous read access), so the graph is visible without an
API key. The single-user API key needed for *writing* (default `secret`) is
printed at startup.
