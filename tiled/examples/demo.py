"""
Build the tree served by `tiled serve demo`.

This is a single, self-contained demo that exercises most of Tiled's features
in one place, so that tutorials and documentation can all point at the same
`tiled serve demo` server:

* A **catalog-backed** tree (SQLite + file storage in a temporary directory),
  rather than a purely in-memory `MapAdapter`. This is what a real Tiled
  deployment looks like, and---importantly---it is what enables the
  experimental **graph of links** feature, whose tables live in the catalog
  database.

* A **showcase** of data structures: arrays and images, tables, sparse,
  awkward, and ragged arrays, an xarray dataset, and structured-dtype arrays.

* A small **provenance graph**: a reduction pipeline of datasets under a
  `linked` container (a `measured` stack and a `background` frame combine
  into `subtracted` -> `normalized` -> `integrated`, with a tabular
  `summary`) wired together with PROV- and RO-Crate-style links, plus
  entities that point at data hosted on *another* Tiled server and at an
  encyclopedic reference for the sample material. The graph definition lives
  in `demo_graph.json` next to this module and is the single editable source
  of the relationships; see the `graph-and-links` user guide.

The public demo is served with anonymous (read-only) access, so the graph and
all data are visible to anyone. A single-user API key (default `"secret"`)
is printed at startup for anyone who wants to try *writing* data or mutating
the graph.
"""

from __future__ import annotations

import atexit
import json
import shutil
import string
import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import numpy

# The default single-user API key for the demo. The demo is public
# (anonymous read access), so this key only matters for *writing* data or
# mutating the graph. A well-known value keeps the tutorials copy-pasteable.
DEMO_API_KEY = "secret"

# The graph definition consumed by `seed_graph`. This JSON-LD-style document
# is the single, editable source of the demo's entities and links.
GRAPH_DEFINITION_PATH = Path(__file__).with_name("demo_graph.json")

# The container under which the provenance-graph datasets are written.
LINKED_CONTAINER = "linked"


def _showcase_data() -> dict:
    """Generate the (deterministic) arrays and tables for the showcase."""
    import random

    random.seed(42)
    rng = numpy.random.default_rng(seed=42)
    data = {
        # Images. Kept modest so the demo starts quickly and does not write
        # hundreds of MB to disk on every launch.
        "big_image": rng.random((1000, 1000)),
        "medium_image": rng.random((500, 500)),
        "small_image": rng.random((300, 300)),
        "tiny_image": rng.random((50, 50)),
        "tiny_cube": rng.random((50, 50, 50)),
        "high_entropy": rng.integers(-10, 10, size=(100, 100)),
        "low_entropy": numpy.ones((100, 100), dtype="int32"),
        "tiny_column": rng.random(10),
        "short_column_int": rng.integers(10, size=10, dtype=numpy.dtype("uint8")),
        "short_column_float": rng.random(10),
        "short_column_bool": numpy.array(random.choices([True, False], k=10)),
        "short_column_datetime": numpy.arange(
            datetime(2025, 1, 1),
            datetime(2025, 1, 11),
            timedelta(days=1),
            dtype="datetime64[D]",
        ),
        "short_column_str": numpy.array(
            random.choices([letter * 3 for letter in string.ascii_letters], k=10),
            dtype="<U3",
        ),
        "long_column": rng.random(10_000),
        "complex": rng.random((30, 50)) + 1j * rng.random((30, 50)),
    }
    return data


def _write_showcase(client) -> None:
    """Write the structure showcase into a (catalog-backed) container."""
    import awkward
    import pandas
    import ragged
    import sparse
    import xarray

    from ..client.xarray import write_xarray_dataset

    data = _showcase_data()

    scalars = client.create_container(
        "scalars", metadata={"numbers": "constants", "precision": 5}
    )
    scalars.write_array(numpy.array(3.14159), key="pi")
    scalars.write_array(numpy.array(42), key="fortytwo")
    scalars.write_array(numpy.array([2.71828]), key="e_arr")

    nested = client.create_container(
        "nested", metadata={"animal": "cat", "color": "green"}
    )
    images = nested.create_container(
        "images", metadata={"animal": "cat", "color": "green"}
    )
    images.write_array(data["tiny_image"], key="tiny_image")
    images.write_array(data["small_image"], key="small_image")
    images.write_array(data["medium_image"], key="medium_image")
    images.write_array(data["big_image"], key="big_image")
    cubes = nested.create_container("cubes", metadata={"animal": "dog", "color": "red"})
    cubes.write_array(data["tiny_cube"], key="tiny_cube")
    nested.write_array(data["complex"], key="complex")

    sparse_arr = data["small_image"].copy()
    sparse_arr[sparse_arr < 0.9] = 0
    coo = sparse.COO(sparse_arr)
    nested.write_sparse(
        coords=coo.coords, data=coo.data, shape=coo.shape, key="sparse_image"
    )

    awkward_arr = awkward.Array(
        [
            [{"x": 1.1, "y": [1]}, {"x": 2.2, "y": [1, 2]}],
            [],
            [{"x": 3.3, "y": [1, 2, 3]}],
        ]
    )
    nested.write_awkward(awkward_arr, key="awkward_array")
    ragged_arr = ragged.array(
        [[[1.1, 2.2, 3.3], []], [[4.4]], [], [[5.5, 6.6, 7.7, 8.8], [9.9]]]
    )
    nested.write_ragged(ragged_arr, key="ragged_array")

    tables = client.create_container("tables")
    tables.write_dataframe(
        pandas.DataFrame(
            {
                "A": data["short_column_int"],
                "B": data["short_column_float"],
                "C": data["short_column_str"],
                "D": data["short_column_datetime"],
                "E": data["short_column_bool"],
            },
            index=pandas.Index(numpy.arange(10), name="index"),
        ),
        key="short_table",
        metadata={"animal": "dog", "color": "red"},
    )
    tables.write_dataframe(
        pandas.DataFrame(
            {
                "A": data["long_column"],
                "B": 2 * data["long_column"],
                "C": 3 * data["long_column"],
            },
            index=pandas.Index(numpy.arange(len(data["long_column"])), name="index"),
        ),
        key="long_table",
        metadata={"animal": "dog", "color": "green"},
    )
    tables.write_dataframe(
        pandas.DataFrame(
            {
                letter: i * data["tiny_column"]
                for i, letter in enumerate(string.ascii_uppercase, start=1)
            },
            index=pandas.Index(numpy.arange(len(data["tiny_column"])), name="index"),
        ),
        key="wide_table",
        metadata={"animal": "dog", "color": "red"},
    )

    structured = client.create_container(
        "structured_data", metadata={"animal": "cat", "color": "green"}
    )
    structured.write_array(
        numpy.array(
            [("Rex", 9, 81.0), ("Fido", 3, 27.0)],
            dtype=[("name", "U10"), ("age", "i4"), ("weight", "f4")],
        ),
        key="pets",
    )
    rng = numpy.random.default_rng(seed=0)
    temp = 15 + 8 * rng.normal(size=(2, 2, 3))
    precip = 10 * rng.uniform(size=(2, 2, 3))
    ds = xarray.Dataset(
        {
            "temperature": (["x", "y", "time"], temp),
            "precipitation": (["x", "y", "time"], precip),
        },
        coords={
            "lon": (["x", "y"], [[-99.83, -99.32], [-99.79, -99.23]]),
            "lat": (["x", "y"], [[42.25, 42.21], [42.63, 42.59]]),
            "time": pandas.date_range("2014-09-06", periods=3),
        },
    )
    write_xarray_dataset(structured, ds, key="xarray_dataset")

    client.write_array(rng.random(100), key="flat_array")
    client.write_array(data["low_entropy"], key="low_entropy")
    client.write_array(data["high_entropy"], key="high_entropy")


def _write_provenance_datasets(client) -> None:
    """Write the datasets that the provenance graph links together.

    These live under a `linked` container, under the names referenced by
    `demo_graph.json` (`measured`, `background`, `subtracted`,
    `normalized`, `integrated`, `summary`), so the graph's entities can
    be tied to them by catalog node id. They form a small, realistic reduction
    pipeline::

        measured - background -> subtracted -> normalized -> integrated
                                                          \\-> summary (table)
    """
    import pandas

    linked = client.create_container(
        LINKED_CONTAINER,
        metadata={
            "description": "Datasets wired together by the provenance graph.",
            "pipeline": [
                "measured",
                "background",
                "subtracted",
                "normalized",
                "integrated",
                "summary",
            ],
        },
    )

    rng = numpy.random.default_rng(seed=7)
    n_frames, n_rows, n_cols = 3, 200, 300
    columns = numpy.arange(n_cols)
    rows = numpy.arange(n_rows)

    # A diffraction-like peak along the column axis, brightening frame to frame.
    peak = numpy.exp(-0.5 * ((columns - 180) / 12.0) ** 2)
    signal = peak[None, None, :] * (1.0 + 0.1 * numpy.arange(n_frames))[:, None, None]

    # A smooth background frame (detector dark current + scattered beam),
    # shared across all frames.
    background = (0.5 + 0.001 * columns[None, :] + 0.0005 * rows[:, None]) * numpy.ones(
        (n_rows, n_cols)
    )
    linked.write_array(
        background,
        key="background",
        metadata={
            "dataset_kind": "background",
            "description": "Dark/scatter background frame, shared across frames.",
            "tiled_uid": "2b1c9f4e-8d3a-4c7b-9e11-2f6a5c8d0b34",
        },
    )

    measured = (
        signal + background[None, :, :] + 0.05 * rng.random((n_frames, n_rows, n_cols))
    )
    linked.write_array(
        measured,
        key="measured",
        metadata={
            "dataset_kind": "measured",
            "sample": "LaB6 calibrant",
            "technique": "x-ray scattering",
            "detector": "Pilatus 300K",
            "frames": n_frames,
            "tiled_uid": "1f0f5e57-6ab8-4e4e-af42-4d907eb85918",
        },
    )

    subtracted = measured - background[None, :, :]
    linked.write_array(
        subtracted,
        key="subtracted",
        metadata={
            "dataset_kind": "derived",
            "operation": "background subtraction",
            "derived_from": ["measured", "background"],
            "tiled_uid": "717aa522-f8ea-49fa-b667-55bc445621f2",
        },
    )

    normalized = (subtracted - subtracted.min()) / (subtracted.max() - subtracted.min())
    linked.write_array(
        normalized,
        key="normalized",
        metadata={
            "dataset_kind": "derived",
            "operation": "normalization",
            "derived_from": "subtracted",
            "tiled_uid": "f083997c-cfbb-44e0-8989-4beec9c717ea",
        },
    )

    # Average over frames and rows to get a 1D profile along the column axis.
    integrated = normalized.mean(axis=(0, 1))
    linked.write_array(
        integrated,
        key="integrated",
        metadata={
            "dataset_kind": "derived",
            "operation": "azimuthal integration",
            "derived_from": "normalized",
            "tiled_uid": "c54e4c14-7f3e-499d-84f6-a4f34fed67d6",
        },
    )

    # A tabular summary of the profile, also derived from `normalized`.
    region = numpy.where(
        columns < n_cols // 3,
        "low",
        numpy.where(columns < 2 * n_cols // 3, "mid", "high"),
    )
    threshold = integrated.mean() + integrated.std()
    summary = pandas.DataFrame(
        {
            "intensity": integrated,
            "cumulative_intensity": numpy.cumsum(integrated),
            "sqrt_intensity": numpy.sqrt(integrated),
            "log_intensity": numpy.log1p(integrated),
            "is_peak": integrated > threshold,
            "region": region,
        },
        index=pandas.Index(columns, name="position"),
    )
    linked.write_dataframe(
        summary,
        key="summary",
        metadata={
            "dataset_kind": "derived",
            "operation": "profile summary table",
            "derived_from": "normalized",
            "columns": list(summary.columns),
            "tiled_uid": "9d4e2a17-6c3b-4f8e-a1d2-7b5c9e0f3a62",
        },
    )


_UPSERT_NAMESPACE = """
mutation($prefix: String!, $uri: String!) {
  upsertNamespace(prefix: $prefix, uri: $uri) { prefix uri }
}
"""
_CREATE_ENTITY = """
mutation($input: CreateEntityInput!) { createEntity(input: $input) { id name } }
"""
_CREATE_LINK = """
mutation($input: CreateLinkInput!) { createLink(input: $input) { id predicate } }
"""
_CATALOG_NODE_ID = "query($path: [String!]!) { catalogNodeId(path: $path) }"


def _graphql(http_client, query: str, variables: dict) -> dict:
    response = http_client.post(
        "/api/graphql", json={"query": query, "variables": variables}
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("errors"):
        raise RuntimeError(f"GraphQL returned errors: {payload['errors']}")
    return payload["data"]


def _load_graph_definition(path: Path) -> tuple[dict[str, str], list[dict], list[dict]]:
    document = json.loads(path.read_text(encoding="utf-8"))
    context = document.get("@context", {})
    namespaces = {
        key: value
        for key, value in context.items()
        if not key.startswith("@")
        and key not in {"subject", "object"}
        and isinstance(value, str)
    }
    entities: list[dict] = []
    links: list[dict] = []
    ids_to_names: dict[str, str] = {}
    for item in document.get("@graph", []):
        if not isinstance(item, dict):
            continue
        if item.get("@type") == "Entity":
            entities.append(item)
            item_id = item.get("@id")
            if isinstance(item_id, str):
                ids_to_names[item_id] = item["name"]
        elif item.get("@type") == "Link":
            links.append(item)
    # Resolve subject/object references (which use @id) to entity names.
    for link in links:
        link["subject"] = ids_to_names.get(link["subject"], link["subject"])
        link["object"] = ids_to_names.get(link["object"], link["object"])
    return namespaces, entities, links


def seed_graph(
    http_client,
    *,
    base_url: str,
    dataset_names: set[str],
    dataset_container: Optional[str] = None,
    definition_path: Optional[Path] = None,
) -> None:
    """Register namespaces and create the demo's entities and links via GraphQL.

    `http_client` must be authenticated with an API key that has
    `write:metadata` scope. `dataset_names` are the entities that
    correspond to real catalog datasets on *this* server; they get their
    `nodeId` resolved from the catalog and a `uri` pointing at this server.
    If `dataset_container` is given, those datasets are looked up under that
    container (e.g. `"linked"` -> path `["linked", name]`).
    """
    definition_path = definition_path or GRAPH_DEFINITION_PATH
    namespaces, entities, links = _load_graph_definition(definition_path)

    for prefix, uri in namespaces.items():
        _graphql(http_client, _UPSERT_NAMESPACE, {"prefix": prefix, "uri": uri})

    entity_ids: dict[str, str] = {}
    for entity in entities:
        name = entity["name"]
        properties = dict(entity.get("properties") or {})
        node_id = entity.get("nodeId")
        uri = entity.get("uri")
        if name in dataset_names:
            path = [dataset_container, name] if dataset_container else [name]
            node_id = _graphql(http_client, _CATALOG_NODE_ID, {"path": path})[
                "catalogNodeId"
            ]
            uri = f"{base_url}/api/v1/metadata/" + "/".join(path)
        entity_input: dict[str, Any] = {
            "entityType": entity.get("entityType", "entity"),
            "name": name,
            "uri": uri,
            "nodeId": node_id,
            "properties": properties,
        }
        created = _graphql(http_client, _CREATE_ENTITY, {"input": entity_input})
        entity_ids[name] = created["createEntity"]["id"]

    for link in links:
        _graphql(
            http_client,
            _CREATE_LINK,
            {
                "input": {
                    "subjectId": entity_ids[link["subject"]],
                    "predicate": link.get("predicate", "relatedTo"),
                    "objectId": entity_ids[link["object"]],
                    "properties": link.get("properties") or {},
                }
            },
        )


# Entities in the graph definition that correspond to real catalog datasets.
DATASET_NAMES = {
    "measured",
    "background",
    "subtracted",
    "normalized",
    "integrated",
    "summary",
}


def _make_catalog(directory: Path, *, init: bool):
    from ..catalog import from_uri as catalog_from_uri

    data_dir = directory / "data"
    data_dir.mkdir(exist_ok=True)
    return catalog_from_uri(
        f"sqlite:///{directory / 'catalog.db'}",
        # File storage for array/table data, plus a SQL (DuckDB) storage
        # backend for the structure families (e.g. ragged arrays) that are
        # stored in a database rather than in files.
        writable_storage=[
            str(data_dir),
            f"duckdb:///{directory / 'data.duckdb'}",
        ],
        init_if_not_exists=init,
    )


def build_demo_tree(
    *,
    base_url: str = "http://127.0.0.1:8000",
    directory: Optional[str] = None,
    api_key: str = DEMO_API_KEY,
):
    """Create, populate, and return a catalog-backed demo tree.

    The datasets and the provenance graph are written into a fresh catalog in a
    temporary directory (cleaned up when the process exits). The returned
    adapter is ready to hand to :func:`tiled.server.app.build_app`.

    Parameters
    ----------
    base_url :
        The URL the server will be reachable at, used to construct the `uri`
        of graph entities that point at data on this server.
    directory :
        Where to place the catalog. Defaults to a temporary directory removed
        at process exit.
    api_key :
        The single-user API key used to authenticate the (in-process) writes
        that populate the demo.
    """
    from ..client import Context, from_context
    from ..config import Authentication
    from ..server.app import build_app

    if directory is None:
        directory = tempfile.mkdtemp(prefix="tiled-demo-")
        atexit.register(shutil.rmtree, directory, ignore_errors=True)
    directory = Path(directory)

    print("Populating demo catalog...", file=sys.stderr)
    catalog = _make_catalog(directory, init=True)
    populate_app = build_app(
        catalog,
        Authentication(single_user_api_key=api_key, allow_anonymous_access=True),
        {},
    )
    with Context.from_app(populate_app) as context:
        client = from_context(context)
        _write_showcase(client)
        _write_provenance_datasets(client)
        seed_graph(
            context.http_client,
            base_url=base_url,
            dataset_names=DATASET_NAMES,
            dataset_container=LINKED_CONTAINER,
        )
    print("Done populating demo catalog.", file=sys.stderr)

    # Return a fresh adapter (the one above had its engine disposed when the
    # populate app's lifespan ended) pointing at the now-populated catalog.
    return _make_catalog(directory, init=False)
