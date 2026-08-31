// Minimal GraphQL client for Tiled's experimental "graph of links" API.
//
// The endpoint lives at /api/graphql (outside /api/v1). We reuse the shared
// axiosInstance so the Authorization header (Bearer token) is attached
// automatically by the auth interceptor; anonymous reads work without one.
import { axiosInstance } from "../client";

export interface GraphEntity {
  id: string;
  name: string;
  entityType: string;
  uri: string | null;
  properties: Record<string, unknown> | null;
  createdAt?: string;
}

export interface GraphLink {
  id: string;
  predicate: string;
  subjectId: string;
  objectId: string;
  properties: Record<string, unknown> | null;
}

export interface GraphData {
  entities: GraphEntity[];
  links: GraphLink[];
}

async function graphql<T>(
  query: string,
  variables: Record<string, unknown>,
  signal?: AbortSignal,
): Promise<T> {
  const response = await axiosInstance.post(
    "/api/graphql",
    { query, variables },
    { signal },
  );
  const payload = response.data;
  if (payload.errors) {
    const message = payload.errors
      .map((e: { message?: string }) => e.message ?? String(e))
      .join("; ");
    throw new Error(message);
  }
  return payload.data as T;
}

const FULL_GRAPH_QUERY = `
query FullGraph($limit: Int!) {
  entities(limit: $limit) {
    id
    name
    entityType
    uri
    properties
    createdAt
  }
  links(limit: $limit) {
    id
    predicate
    subjectId
    objectId
    properties
  }
}
`;

// Fetch the whole graph in one round trip. Suitable for the modest graphs the
// feature currently targets; a lazy per-node expansion could replace this for
// very large graphs (see fetchNeighborhood).
export const fetchFullGraph = async (
  signal?: AbortSignal,
  limit = 500,
): Promise<GraphData> => {
  return graphql<GraphData>(FULL_GRAPH_QUERY, { limit }, signal);
};

const NEIGHBORHOOD_QUERY = `
query Neighborhood($id: ID!) {
  entity(id: $id) {
    id
    name
    entityType
    uri
    properties
    createdAt
    outgoingLinks {
      id
      predicate
      subjectId
      objectId
      properties
      object { id name entityType uri properties }
    }
    incomingLinks {
      id
      predicate
      subjectId
      objectId
      properties
      subject { id name entityType uri properties }
    }
  }
}
`;

interface NeighborhoodLink extends GraphLink {
  object?: GraphEntity | null;
  subject?: GraphEntity | null;
}

interface NeighborhoodResponse {
  entity:
    | (GraphEntity & {
        outgoingLinks: NeighborhoodLink[];
        incomingLinks: NeighborhoodLink[];
      })
    | null;
}

// Fetch a single entity plus its immediate neighbors (one hop out and in).
// Used to lazily grow the graph when a node is expanded.
export const fetchNeighborhood = async (
  id: string,
  signal?: AbortSignal,
): Promise<GraphData | null> => {
  const data = await graphql<NeighborhoodResponse>(
    NEIGHBORHOOD_QUERY,
    { id },
    signal,
  );
  const center = data.entity;
  if (!center) return null;

  const entities: GraphEntity[] = [
    {
      id: center.id,
      name: center.name,
      entityType: center.entityType,
      uri: center.uri,
      properties: center.properties,
      createdAt: center.createdAt,
    },
  ];
  const links: GraphLink[] = [];

  for (const link of center.outgoingLinks) {
    if (link.object) entities.push(link.object);
    links.push({
      id: link.id,
      predicate: link.predicate,
      subjectId: link.subjectId,
      objectId: link.objectId,
      properties: link.properties,
    });
  }
  for (const link of center.incomingLinks) {
    if (link.subject) entities.push(link.subject);
    links.push({
      id: link.id,
      predicate: link.predicate,
      subjectId: link.subjectId,
      objectId: link.objectId,
      properties: link.properties,
    });
  }
  return { entities, links };
};
