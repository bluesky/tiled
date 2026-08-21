The Graph of Links feature is experimental. The APIs may change.

# Entity/Link Graphs

Alongside its tree of datasets, a catalog-backed Tiled server can optionally
serve a graph of **entities** and **links** between them, queryable through a
GraphQL API.

## What problem this solves

The Tiled catalog is very good at describing *what a dataset is*---its
metadata, structure, format, and location (see {doc}`catalog`). It has no
built-in way to describe *how datasets relate to one another*: which dataset
was derived from which, which files belong to the same experiment, which
workflow produced which result. Those relationships are exactly what a graph
model is for.

Rather than inventing a bespoke way to encode relationships as metadata
fields, Tiled adds a small, general-purpose graph layer next to the catalog:

- An **entity** is a named node with a type and arbitrary JSON properties. An
  entity may optionally reference a specific node in the Tiled catalog tree
  (via `node_id`), tying the graph directly to hosted data, but it does not
  have to---entities can also represent things Tiled does not otherwise track,
  like external instruments, people, or software.
- A **link** is a directed, predicate-labeled edge between two entities:
  *(subject, predicate, object)*, plus its own arbitrary JSON properties.

This is the classic **subject-predicate-object triple** at the heart of
[RDF](https://www.w3.org/RDF/) and triplestores. Tiled's implementation is
intentionally a *partial* one: entities and links are stored in ordinary SQL
tables (in the same database as the catalog), not in a dedicated RDF/triple
store, and there is no SPARQL endpoint, no inference/reasoning, and no
first-class notion of blank nodes or named graphs. What it does provide is:

- Typed, queryable nodes and edges, with pagination and filtering.
- Arbitrary JSON properties on both entities and links.
- The same tag/user-based access control model used elsewhere in Tiled,
  applied per-entity and per-link (see {doc}`access-control`).
- A **namespace registry** that resolves CURIEs. A
  [CURIE](https://www.w3.org/TR/curie/) ("Compact URI") is a shorthand for a
  full URI, built from a registered prefix and a local name, separated by a
  colon---the same idea as an XML namespace prefix. For example, if the
  prefix `schema` is registered to `https://schema.org/`, then the CURIE
  `schema:name` is shorthand for the full, unambiguous IRI
  `https://schema.org/name`. This lets property keys and link predicates be
  written compactly (`schema:name`) while Tiled stores the full IRI
  internally and compacts it back to a CURIE whenever it's read through the
  GraphQL API, so there is never any ambiguity about which `name` (or
  `wasDerivedFrom`, or `hasPart`) a given term refers to.

## Why this matters for RO and PROV

Because the graph lets you register *any* namespace and consistently
expands/compacts CURIEs against it, it is a natural fit for existing,
widely-used vocabularies rather than an ad hoc scheme invented per-deployment:

- [**PROV**](https://www.w3.org/TR/prov-o/) (the W3C provenance ontology)
  defines terms like `wasDerivedFrom`, `wasGeneratedBy`, `used`, `Agent`,
  and `Activity` for describing how one piece of data came from another. A
  processing pipeline can record `subtracted -[prov:wasDerivedFrom]->
  measured` and `analysis_workflow -[prov:used]-> measured` using terms
  that provenance-aware tools elsewhere already understand, instead of a
  Tiled-specific `derived_from` metadata field with no agreed-upon meaning.
- [**RO-Crate**](https://www.researchobject.org/ro-crate/) (and the underlying
  [RO Terms](https://w3id.org/ro/terms/) vocabulary) is a lightweight
  packaging convention for describing a research output---a dataset,
  workflow, or software package---as a self-describing bundle of files plus
  metadata. Its `hasPart` relationship (which of the assets an
  `ro-crate-metadata.json` file describes) maps directly onto a Tiled link
  between entities, using a predicate that RO-Crate-aware tooling already
  recognizes.

In short: the graph does not require RO or PROV, but registering their
namespaces and using their predicates means links created in Tiled use the
same vocabulary as the broader ecosystem of provenance- and
research-object-aware tools, rather than an ad hoc scheme legible only to
Tiled itself.

## What it is not

To set expectations clearly, the graph feature does **not**:

- Provide a SPARQL endpoint or general RDF query language---only the specific
  filters exposed by the GraphQL schema (by subject, predicate, object, entity
  type) and simple pagination.
- Perform any inference, reasoning, or validation against an ontology.
  Registering the `prov` namespace does not teach Tiled anything about what
  `wasDerivedFrom` means; it only lets that term round-trip through CURIE
  expansion/compaction correctly.
- Store data as RDF triples internally---entities and links live in ordinary
  relational tables in the catalog database, alongside the catalog's own
  tables.

See {doc}`../user-guide/graph-and-links` for how to explore an existing graph or build
one of your own.
