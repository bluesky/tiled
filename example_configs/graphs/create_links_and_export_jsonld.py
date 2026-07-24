from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import httpx
from create_datasets import DATASETS

CREATE_ENTITY_MUTATION = """
mutation($input: CreateEntityInput!) {
  createEntity(input: $input) { id name }
}
"""

CREATE_LINK_MUTATION = """
mutation($input: CreateLinkInput!) {
  createLink(input: $input) { id predicate }
}
"""

LIST_ENTITIES_QUERY = """
query {
    entities(limit: 500) {
        id
        name
    }
}
"""

LIST_MATCHING_LINKS_QUERY = """
query($subjectId: ID!, $predicate: String!, $objectId: ID!) {
    links(subjectId: $subjectId, predicate: $predicate, objectId: $objectId, limit: 10) {
        id
    }
}
"""

CATALOG_NODE_ID_QUERY = """
query($path: [String!]!) {
    catalogNodeId(path: $path)
}
"""

DATASET_NAMES = {dataset["name"] for dataset in DATASETS}


def _post_graphql(client: httpx.Client, query: str, variables: dict) -> dict:
    response = client.post(
        "/api/graphql",
        json={"query": query, "variables": variables},
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("errors"):
        raise RuntimeError(f"GraphQL returned errors: {payload['errors']}")
    return payload


def _load_input_document(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("Input must be a JSON object.")
    graph = payload.get("@graph")
    if not isinstance(graph, list):
        raise RuntimeError("Input JSON-LD must include '@graph' as a list.")
    return payload


def _resolve_entity_name(ref: str, entities_by_id: dict[str, str]) -> str:
    if ref in DATASET_NAMES:
        return ref
    if ref in entities_by_id:
        return entities_by_id[ref]
    return ref


def main() -> None:
    base_url = os.getenv("TILED_BASE_URL", "http://127.0.0.1:8000")
    api_key = os.getenv("TILED_API_KEY", "secret")
    output_path = Path(
        os.getenv(
            "GRAPH_JSONLD_OUTPUT",
            "example_configs/graphs/exported_graph.jsonld",
        )
    )
    input_path = Path(
        os.getenv(
            "GRAPH_JSONLD_INPUT",
            "example_configs/graphs/input.json",
        )
    )

    document = _load_input_document(input_path)
    graph_items = document["@graph"]

    headers = {"Authorization": f"Apikey {api_key}"}
    entities: dict[str, dict[str, Any]] = {}
    links: list[dict[str, Any]] = []
    entities_by_id: dict[str, str] = {}

    for item in graph_items:
        if not isinstance(item, dict):
            continue
        item_type = item.get("@type")
        if item_type == "Entity":
            name = item.get("name")
            if not isinstance(name, str) or not name:
                raise RuntimeError(
                    "Each Entity in input JSON-LD must have a non-empty 'name'."
                )

            if "properties" in item and not isinstance(item["properties"], dict):
                raise RuntimeError(f"Entity '{name}' has non-object properties.")

            properties = dict(item.get("properties") or {})

            if name in DATASET_NAMES:
                # Points at data hosted by this server: link to it by its
                # full Tiled URL rather than an ad hoc relative path.
                uri = f"{base_url}/api/v1/metadata/{name}"
            else:
                # Points elsewhere (e.g. a dataset on another Tiled server)
                # if the input document says so, or nowhere at all otherwise.
                uri = item.get("uri")

            entity_input = {
                "entityType": item.get("entityType", "entity"),
                "name": name,
                "uri": uri,
                "nodeId": item.get("nodeId"),
                "properties": properties,
            }
            if "accessBlob" in item:
                entity_input["accessBlob"] = item["accessBlob"]

            entities[name] = entity_input
            item_id = item.get("@id")
            if isinstance(item_id, str):
                entities_by_id[item_id] = name

        elif item_type == "Link":
            subject = item.get("subject")
            object_ = item.get("object")
            if not isinstance(subject, str) or not isinstance(object_, str):
                raise RuntimeError(
                    "Each Link in input JSON-LD must have string 'subject' and 'object'."
                )
            if "properties" in item and not isinstance(item["properties"], dict):
                raise RuntimeError(
                    "Each Link properties value must be an object when present."
                )

            link_input = {
                "subject": _resolve_entity_name(subject, entities_by_id),
                "predicate": item.get("predicate", "relatedTo"),
                "object": _resolve_entity_name(object_, entities_by_id),
                "properties": item.get("properties") or {},
            }
            if "accessBlob" in item:
                link_input["accessBlob"] = item["accessBlob"]
            links.append(link_input)

    with httpx.Client(base_url=base_url, headers=headers, timeout=30.0) as client:
        # Persist namespace prefixes in the graph store for future exports.
        client.post(
            "/api/v1/graph/jsonld",
            json={"@context": document.get("@context", {}), "@graph": []},
        ).raise_for_status()

        entity_ids: dict[str, str] = {}

        # For entities that are real catalog datasets, resolve the catalog
        # node's internal id so it lands in the entity's `nodeId` (and thus
        # the `node_id` column), instead of being duplicated into `properties`.
        for name, entity in entities.items():
            if entity["nodeId"] is None and name in DATASET_NAMES:
                lookup = _post_graphql(client, CATALOG_NODE_ID_QUERY, {"path": [name]})
                node_id = lookup["data"]["catalogNodeId"]
                if node_id is None:
                    raise RuntimeError(
                        f"Could not resolve catalog node id for dataset '{name}'."
                    )
                entity["nodeId"] = node_id

        existing = _post_graphql(client, LIST_ENTITIES_QUERY, {})
        for entity in existing["data"]["entities"]:
            if entity["name"] in entities and entity["name"] not in entity_ids:
                entity_ids[entity["name"]] = entity["id"]

        for name, entity in entities.items():
            if name in entity_ids:
                print(
                    f"Graph entity already exists, skipping: {name} -> {entity_ids[name]}"
                )
                continue
            payload = _post_graphql(
                client,
                CREATE_ENTITY_MUTATION,
                {"input": entity},
            )
            entity_ids[name] = payload["data"]["createEntity"]["id"]
            print(f"Created graph entity: {name} -> {entity_ids[name]}")

        for link in links:
            subject_name = link["subject"]
            object_name = link["object"]
            predicate = link["predicate"]

            if subject_name not in entity_ids:
                raise RuntimeError(
                    f"Link subject '{subject_name}' not found among graph entities."
                )
            if object_name not in entity_ids:
                raise RuntimeError(
                    f"Link object '{object_name}' not found among graph entities."
                )

            check_payload = _post_graphql(
                client,
                LIST_MATCHING_LINKS_QUERY,
                {
                    "subjectId": entity_ids[subject_name],
                    "predicate": predicate,
                    "objectId": entity_ids[object_name],
                },
            )
            if check_payload["data"]["links"]:
                print(
                    f"Link already exists: {subject_name} -[{predicate}]-> {object_name}"
                )
                continue
            payload = _post_graphql(
                client,
                CREATE_LINK_MUTATION,
                {
                    "input": {
                        "subjectId": entity_ids[subject_name],
                        "predicate": predicate,
                        "objectId": entity_ids[object_name],
                        "properties": link["properties"],
                        **(
                            {"accessBlob": link["accessBlob"]}
                            if "accessBlob" in link
                            else {}
                        ),
                    }
                },
            )
            print(
                "Created link: "
                f"{subject_name} -[{predicate}]-> {object_name} "
                f"(id={payload['data']['createLink']['id']})"
            )

        export_response = client.get("/api/v1/graph/jsonld")
        export_response.raise_for_status()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(export_response.json(), indent=2),
            encoding="utf-8",
        )
        print(f"Exported JSON-LD to: {output_path}")


if __name__ == "__main__":
    main()
