// Lay out React Flow nodes/edges as a directed graph using dagre.
import dagre from "@dagrejs/dagre";
import { Node, Edge, Position } from "@xyflow/react";

const NODE_WIDTH = 180;
const NODE_HEIGHT = 48;

export type LayoutDirection = "LR" | "TB";

export function layoutGraph(
  nodes: Node[],
  edges: Edge[],
  direction: LayoutDirection = "LR",
): Node[] {
  const g = new dagre.graphlib.Graph();
  g.setDefaultEdgeLabel(() => ({}));
  g.setGraph({ rankdir: direction, nodesep: 40, ranksep: 90 });

  nodes.forEach((node) => {
    g.setNode(node.id, { width: NODE_WIDTH, height: NODE_HEIGHT });
  });
  edges.forEach((edge) => {
    g.setEdge(edge.source, edge.target);
  });

  dagre.layout(g);

  const horizontal = direction === "LR";
  return nodes.map((node) => {
    const { x, y } = g.node(node.id);
    return {
      ...node,
      sourcePosition: horizontal ? Position.Right : Position.Bottom,
      targetPosition: horizontal ? Position.Left : Position.Top,
      // dagre gives us the node center; React Flow wants the top-left corner.
      position: { x: x - NODE_WIDTH / 2, y: y - NODE_HEIGHT / 2 },
    };
  });
}

export { NODE_WIDTH, NODE_HEIGHT };
