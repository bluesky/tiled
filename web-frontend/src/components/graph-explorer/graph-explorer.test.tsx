import { vi, describe, it, expect, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/react";
import "@testing-library/jest-dom";

import GraphExplorer, {
  buildElements,
  colorFor,
  dataHref,
  entityTypesIn,
  isExternalEntity,
  nodeColor,
  DEFAULT_HIDDEN_TYPES,
} from "./graph-explorer";
import { fetchFullGraph, GraphData } from "../../graph/graph-client";

// Stub React Flow: rendering the real canvas requires layout measurements that
// jsdom does not provide. We expose just enough to assert on node/edge counts
// and to drive the click handlers.
vi.mock("@xyflow/react", async () => {
  const React = await import("react");
  return {
    ReactFlow: ({ nodes, edges, onNodeClick, onEdgeClick, children }: any) => (
      <div data-testid="react-flow">
        <div data-testid="node-count">{nodes.length}</div>
        <div data-testid="edge-count">{edges.length}</div>
        {nodes.map((n: any) => (
          <button
            key={n.id}
            data-testid={`node-${n.id}`}
            onClick={(e) => onNodeClick(e, n)}
          >
            {n.data.label}
          </button>
        ))}
        {edges.map((edge: any) => (
          <button
            key={edge.id}
            data-testid={`edge-${edge.id}`}
            onClick={(e) => onEdgeClick(e, edge)}
          >
            {edge.label}
          </button>
        ))}
        {children}
      </div>
    ),
    Background: () => null,
    Controls: () => null,
    MiniMap: () => null,
    MarkerType: { ArrowClosed: "arrowclosed" },
    Position: { Left: "left", Right: "right", Top: "top", Bottom: "bottom" },
    useNodesState: (initial: any) => {
      const [state, setState] = React.useState(initial);
      return [state, setState, () => {}];
    },
    useEdgesState: (initial: any) => {
      const [state, setState] = React.useState(initial);
      return [state, setState, () => {}];
    },
  };
});

// Keep layout deterministic and free of dagre in the component test.
vi.mock("../../graph/layout", () => ({
  layoutGraph: (nodes: any) => nodes,
}));

vi.mock("../../graph/graph-client", () => ({
  fetchFullGraph: vi.fn(),
}));

const fetchFullGraphMock = vi.mocked(fetchFullGraph);

const sampleGraph: GraphData = {
  entities: [
    {
      id: "measured",
      name: "measured",
      entityType: "dataset",
      uri: "http://example/measured",
      properties: { shape: [200, 300] },
    },
    {
      id: "workflow",
      name: "reduce",
      entityType: "workflow",
      uri: null,
      properties: null,
    },
  ],
  links: [
    {
      id: "link-1",
      predicate: "wasGeneratedBy",
      subjectId: "measured",
      objectId: "workflow",
      properties: { role: "primary" },
    },
    // Dangling link: object entity is not present -> must be dropped.
    {
      id: "link-2",
      predicate: "used",
      subjectId: "workflow",
      objectId: "ghost",
      properties: null,
    },
  ],
};

describe("buildElements", () => {
  it("maps entities to nodes and keeps only links between visible entities", () => {
    const { nodes, edges } = buildElements(sampleGraph);

    expect(nodes.map((n) => n.id).sort()).toEqual(["measured", "workflow"]);
    // link-2 references a missing "ghost" entity and is filtered out.
    expect(edges.map((e) => e.id)).toEqual(["link-1"]);
    expect(edges[0]).toMatchObject({ source: "measured", target: "workflow" });
  });
});

describe("colorFor", () => {
  it("returns a distinct color per known entity type", () => {
    expect(colorFor("dataset")).not.toBe(colorFor("workflow"));
  });

  it("falls back to the default color for unknown types", () => {
    expect(colorFor("mystery")).toBe(colorFor("also-unknown"));
  });
});

describe("dataHref", () => {
  const origin = window.location.origin;
  const host = window.location.hostname;
  const port = window.location.port;
  const proto = window.location.protocol;

  it("rewrites a local Tiled metadata URL to the UI browse page", () => {
    expect(dataHref(`${origin}/api/v1/metadata/linked/measured`)).toBe(
      "/ui/browse/linked/measured",
    );
  });

  it("preserves a query string (e.g. slice or column selection)", () => {
    expect(
      dataHref(`${origin}/api/v1/metadata/linked/summary?field=phase`),
    ).toBe("/ui/browse/linked/summary?field=phase");
  });

  it("treats loopback host aliases as the same server", () => {
    // Only meaningful when the test host is a loopback alias.
    if (host !== "localhost" && host !== "127.0.0.1") return;
    const alias = host === "localhost" ? "127.0.0.1" : "localhost";
    const uri = `${proto}//${alias}${port ? ":" + port : ""}/api/v1/metadata/linked/measured`;
    expect(dataHref(uri)).toBe("/ui/browse/linked/measured");
  });

  it("rewrites an external Tiled server's metadata URL to its browse page", () => {
    const external =
      "https://tiled-demo.nsls2.bnl.gov/api/v1/metadata/csx/abc/primary/img";
    expect(dataHref(external)).toBe(
      "https://tiled-demo.nsls2.bnl.gov/ui/browse/csx/abc/primary/img",
    );
  });

  it("leaves a non-Tiled reference URI unchanged", () => {
    const wikidata = "http://www.wikidata.org/entity/Q410318";
    expect(dataHref(wikidata)).toBe(wikidata);
  });

  it("leaves a source-code URI (e.g. a GitHub link) unchanged", () => {
    const script =
      "https://github.com/genematx/tiled/blob/graph-web-ui/docs/demo/processing_script.py";
    expect(dataHref(script)).toBe(script);
  });

  it("returns unparseable input verbatim", () => {
    expect(dataHref("not a url")).toBe("not a url");
  });
});

const localEntity = (uri: string | null) => ({
  id: "x",
  name: "x",
  entityType: "dataset",
  uri,
  properties: null,
});

describe("isExternalEntity / nodeColor", () => {
  const origin = window.location.origin;

  it("treats a same-server dataset URI as local", () => {
    const e = localEntity(`${origin}/api/v1/metadata/linked/measured`);
    expect(isExternalEntity(e)).toBe(false);
    expect(nodeColor(e)).toBe(colorFor("dataset"));
  });

  it("treats a dataset URI on another origin as external", () => {
    const e = localEntity(
      "https://tiled-demo.nsls2.bnl.gov/api/v1/metadata/csx/abc/primary/img",
    );
    expect(isExternalEntity(e)).toBe(true);
    // External datasets get a distinct color, not the local dataset color.
    expect(nodeColor(e)).not.toBe(colorFor("dataset"));
  });

  it("does not recolor non-dataset external entities (references keep their color)", () => {
    const ref = {
      id: "r",
      name: "LaB6",
      entityType: "reference",
      uri: "http://www.wikidata.org/entity/Q410318",
      properties: null,
    };
    expect(isExternalEntity(ref)).toBe(true);
    expect(nodeColor(ref)).toBe(colorFor("reference"));
  });

  it("does not recolor external software entities (a GitHub-hosted script keeps its color)", () => {
    const software = {
      id: "s",
      name: "processing_script.py",
      entityType: "software",
      uri: "https://github.com/genematx/tiled/blob/graph-web-ui/docs/demo/processing_script.py",
      properties: null,
    };
    expect(isExternalEntity(software)).toBe(true);
    expect(nodeColor(software)).toBe(colorFor("software"));
  });

  it("treats an entity without a URI as local", () => {
    expect(isExternalEntity(localEntity(null))).toBe(false);
  });
});

describe("entityTypesIn", () => {
  it("lists the types present, known ones first then extras alphabetically", () => {
    const data: GraphData = {
      entities: [
        { id: "1", name: "a", entityType: "widget", uri: null, properties: null },
        { id: "2", name: "b", entityType: "workflow", uri: null, properties: null },
        { id: "3", name: "c", entityType: "dataset", uri: null, properties: null },
        { id: "4", name: "d", entityType: "apparatus", uri: null, properties: null },
      ],
      links: [],
    };

    // dataset + workflow are known (legend order); apparatus + widget are extras.
    expect(entityTypesIn(data)).toEqual([
      "dataset",
      "workflow",
      "apparatus",
      "widget",
    ]);
  });
});

describe("GraphExplorer", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("shows a loading skeleton before data arrives", () => {
    fetchFullGraphMock.mockReturnValue(new Promise(() => {}));

    const { container } = render(<GraphExplorer />);

    expect(container.querySelector(".MuiSkeleton-root")).toBeInTheDocument();
  });

  it("renders nodes and the surviving edges once loaded", async () => {
    fetchFullGraphMock.mockResolvedValue(sampleGraph);

    render(<GraphExplorer />);

    await waitFor(() => {
      expect(screen.getByTestId("node-count")).toHaveTextContent("2");
    });
    expect(screen.getByTestId("edge-count")).toHaveTextContent("1");
  });

  it("populates the detail panel when a node is clicked", async () => {
    fetchFullGraphMock.mockResolvedValue(sampleGraph);

    render(<GraphExplorer />);

    await waitFor(() =>
      expect(screen.getByTestId("node-measured")).toBeInTheDocument(),
    );
    fireEvent.click(screen.getByTestId("node-measured"));

    // The URI is a pictogram link (opens in a new tab), not printed text.
    const link = screen.getByRole("link", {
      name: "Open http://example/measured",
    });
    expect(link).toHaveAttribute("href", "http://example/measured");
    expect(link).toHaveAttribute("target", "_blank");
    expect(screen.getByText(/"shape"/)).toBeInTheDocument();
  });

  it("shows an error alert when the fetch fails", async () => {
    fetchFullGraphMock.mockRejectedValue(new Error("network down"));

    render(<GraphExplorer />);

    await waitFor(() =>
      expect(
        screen.getByText(/Failed to load graph: network down/),
      ).toBeInTheDocument(),
    );
  });

  it("shows an informational alert when the graph is empty", async () => {
    fetchFullGraphMock.mockResolvedValue({ entities: [], links: [] });

    render(<GraphExplorer />);

    await waitFor(() =>
      expect(screen.getByText(/No entities are visible/)).toBeInTheDocument(),
    );
  });
});

const graphWithCrate: GraphData = {
  entities: [
    {
      id: "measured",
      name: "measured",
      entityType: "dataset",
      uri: null,
      properties: null,
    },
    {
      id: "crate",
      name: "crate",
      entityType: "rocrate",
      uri: null,
      properties: null,
    },
  ],
  links: [
    {
      id: "hasPart",
      predicate: "hasPart",
      subjectId: "crate",
      objectId: "measured",
      properties: null,
    },
  ],
};

describe("GraphExplorer type visibility", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("hides the default-hidden types (rocrate) on first render", async () => {
    expect(DEFAULT_HIDDEN_TYPES).toContain("rocrate");
    fetchFullGraphMock.mockResolvedValue(graphWithCrate);

    render(<GraphExplorer />);

    await waitFor(() =>
      expect(screen.getByTestId("node-measured")).toBeInTheDocument(),
    );
    // The rocrate node and its only edge are hidden.
    expect(screen.queryByTestId("node-crate")).not.toBeInTheDocument();
    expect(screen.getByTestId("node-count")).toHaveTextContent("1");
    expect(screen.getByTestId("edge-count")).toHaveTextContent("0");
  });

  it("reveals a hidden type when its legend chip is clicked", async () => {
    fetchFullGraphMock.mockResolvedValue(graphWithCrate);

    render(<GraphExplorer />);

    await waitFor(() =>
      expect(screen.getByTestId("node-measured")).toBeInTheDocument(),
    );

    fireEvent.click(screen.getByText("rocrate"));

    await waitFor(() =>
      expect(screen.getByTestId("node-crate")).toBeInTheDocument(),
    );
    expect(screen.getByTestId("node-count")).toHaveTextContent("2");
    expect(screen.getByTestId("edge-count")).toHaveTextContent("1");
  });

  it("hides a visible type when its legend chip is clicked", async () => {
    fetchFullGraphMock.mockResolvedValue(graphWithCrate);

    render(<GraphExplorer />);

    await waitFor(() =>
      expect(screen.getByTestId("node-measured")).toBeInTheDocument(),
    );

    fireEvent.click(screen.getByText("dataset"));

    await waitFor(() =>
      expect(screen.queryByTestId("node-measured")).not.toBeInTheDocument(),
    );
  });
});
