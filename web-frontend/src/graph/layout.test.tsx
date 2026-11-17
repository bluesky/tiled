import { describe, it, expect } from "vitest";
import { Node, Edge, Position } from "@xyflow/react";
import { layoutGraph, NODE_WIDTH, NODE_HEIGHT } from "./layout";

const nodes: Node[] = [
  { id: "a", position: { x: 0, y: 0 }, data: {} },
  { id: "b", position: { x: 0, y: 0 }, data: {} },
  { id: "c", position: { x: 0, y: 0 }, data: {} },
];
const edges: Edge[] = [
  { id: "e1", source: "a", target: "b" },
  { id: "e2", source: "b", target: "c" },
];

describe("layoutGraph", () => {
  it("assigns a finite position to every node", () => {
    const laid = layoutGraph(nodes, edges);

    expect(laid).toHaveLength(3);
    for (const node of laid) {
      expect(Number.isFinite(node.position.x)).toBe(true);
      expect(Number.isFinite(node.position.y)).toBe(true);
    }
  });

  it("uses horizontal handles for the LR direction", () => {
    const laid = layoutGraph(nodes, edges, "LR");

    for (const node of laid) {
      expect(node.sourcePosition).toBe(Position.Right);
      expect(node.targetPosition).toBe(Position.Left);
    }
  });

  it("uses vertical handles for the TB direction", () => {
    const laid = layoutGraph(nodes, edges, "TB");

    for (const node of laid) {
      expect(node.sourcePosition).toBe(Position.Bottom);
      expect(node.targetPosition).toBe(Position.Top);
    }
  });

  it("separates connected nodes along the primary axis", () => {
    const laid = layoutGraph(nodes, edges, "LR");
    const byId = Object.fromEntries(laid.map((n) => [n.id, n]));

    // b follows a, c follows b when flowing left to right.
    expect(byId.b.position.x).toBeGreaterThan(byId.a.position.x);
    expect(byId.c.position.x).toBeGreaterThan(byId.b.position.x);
  });

  it("exposes the node dimensions used for layout", () => {
    expect(NODE_WIDTH).toBeGreaterThan(0);
    expect(NODE_HEIGHT).toBeGreaterThan(0);
  });
});
