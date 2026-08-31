import { vi, describe, it, expect, beforeEach } from "vitest";
import { axiosInstance } from "../client";
import { fetchFullGraph, fetchNeighborhood } from "./graph-client";

vi.mock("../client", () => ({
  axiosInstance: { post: vi.fn() },
}));

const post = vi.mocked(axiosInstance.post);

describe("fetchFullGraph", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("posts to the /api/graphql endpoint and returns the data payload", async () => {
    const data = {
      entities: [
        {
          id: "a",
          name: "A",
          entityType: "dataset",
          uri: null,
          properties: null,
        },
      ],
      links: [],
    };
    post.mockResolvedValue({ data: { data } });

    const result = await fetchFullGraph();

    expect(result).toEqual(data);
    const [url, body] = post.mock.calls[0];
    expect(url).toBe("/api/graphql");
    expect(body).toMatchObject({ variables: { limit: 500 } });
    expect((body as { query: string }).query).toContain("query FullGraph");
  });

  it("passes a custom limit through as a GraphQL variable", async () => {
    post.mockResolvedValue({ data: { data: { entities: [], links: [] } } });

    await fetchFullGraph(undefined, 42);

    expect(post.mock.calls[0][1]).toMatchObject({ variables: { limit: 42 } });
  });

  it("throws with the joined GraphQL error messages", async () => {
    post.mockResolvedValue({
      data: { errors: [{ message: "boom" }, { message: "kaboom" }] },
    });

    await expect(fetchFullGraph()).rejects.toThrow("boom; kaboom");
  });
});

describe("fetchNeighborhood", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("flattens outgoing and incoming links into entities and links", async () => {
    const center = {
      id: "center",
      name: "Center",
      entityType: "dataset",
      uri: null,
      properties: null,
      createdAt: "2025-01-01",
      outgoingLinks: [
        {
          id: "l1",
          predicate: "derivedFrom",
          subjectId: "center",
          objectId: "down",
          properties: null,
          object: {
            id: "down",
            name: "Down",
            entityType: "dataset",
            uri: null,
            properties: null,
          },
        },
      ],
      incomingLinks: [
        {
          id: "l2",
          predicate: "produces",
          subjectId: "up",
          objectId: "center",
          properties: null,
          subject: {
            id: "up",
            name: "Up",
            entityType: "workflow",
            uri: null,
            properties: null,
          },
        },
      ],
    };
    post.mockResolvedValue({ data: { data: { entity: center } } });

    const result = await fetchNeighborhood("center");

    expect(result).not.toBeNull();
    expect(result!.entities.map((e) => e.id).sort()).toEqual([
      "center",
      "down",
      "up",
    ]);
    expect(result!.links.map((l) => l.id).sort()).toEqual(["l1", "l2"]);
    // Nested object/subject entities are not carried onto the flat links.
    expect(result!.links[0]).not.toHaveProperty("object");
  });

  it("returns null when the entity is not found", async () => {
    post.mockResolvedValue({ data: { data: { entity: null } } });

    expect(await fetchNeighborhood("missing")).toBeNull();
  });
});
