import { useCallback, useEffect, useMemo, useState } from "react";
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  MarkerType,
  useNodesState,
  useEdgesState,
  Node,
  Edge,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";

import Alert from "@mui/material/Alert";
import Box from "@mui/material/Box";
import Chip from "@mui/material/Chip";
import IconButton from "@mui/material/IconButton";
import Paper from "@mui/material/Paper";
import Skeleton from "@mui/material/Skeleton";
import Stack from "@mui/material/Stack";
import ToggleButton from "@mui/material/ToggleButton";
import ToggleButtonGroup from "@mui/material/ToggleButtonGroup";
import Tooltip from "@mui/material/Tooltip";
import Typography from "@mui/material/Typography";
import OpenInNewIcon from "@mui/icons-material/OpenInNew";

import {
  fetchFullGraph,
  GraphData,
  GraphEntity,
  GraphLink,
} from "../../graph/graph-client";
import { layoutGraph, LayoutDirection } from "../../graph/layout";

// Color-code nodes by entity type. Unknown types fall back to grey.
const ENTITY_COLORS: Record<string, string> = {
  dataset: "#1976d2",
  reference: "#7b1fa2",
  workflow: "#2e7d32",
  software: "#ed6c02",
  rocrate: "#00838f",
};
const DEFAULT_COLOR = "#616161";

// A dataset hosted on another Tiled server gets a distinct (darker) shade of
// blue so it reads as "the same kind of thing, elsewhere".
const EXTERNAL_DATASET_COLOR = "#0d47a1";

// Entity types hidden on first load. The RO-Crate manifest node connects to
// every dataset and mostly adds clutter, so we hide it until asked for.
export const DEFAULT_HIDDEN_TYPES = ["rocrate"];

export function colorFor(entityType: string): string {
  return ENTITY_COLORS[entityType] ?? DEFAULT_COLOR;
}

// The entity types actually present in the graph, ordered with the known types
// first (in legend order) and any unrecognized types appended alphabetically.
export function entityTypesIn(data: GraphData): string[] {
  const present = new Set(data.entities.map((e) => e.entityType));
  const known = Object.keys(ENTITY_COLORS).filter((t) => present.has(t));
  const extra = [...present]
    .filter((t) => !(t in ENTITY_COLORS))
    .sort((a, b) => a.localeCompare(b));
  return [...known, ...extra];
}

// Is this URL served by the same Tiled server as the running UI? Loopback
// aliases (127.0.0.1 / [::1] / localhost) are treated as equivalent so the
// demo's 127.0.0.1 URIs still match a localhost browser session.
function isSameServer(url: URL): boolean {
  const loc = window.location;
  const normHost = (host: string) =>
    host === "127.0.0.1" || host === "[::1]" || host === "::1"
      ? "localhost"
      : host;
  return (
    url.protocol === loc.protocol &&
    (url.port || "") === (loc.port || "") &&
    normHost(url.hostname) === normHost(loc.hostname)
  );
}

// Resolve an entity URI to the best clickable destination. Any Tiled metadata
// URL (`.../api/v1/metadata/<path>`) — on this server or another one — is
// rewritten to that server's UI browse page (`.../ui/browse/<path>`) so the
// link opens the actual data (array, table, or container) instead of a
// metadata document. Query strings and fragments (e.g. a slice or column
// selection) are preserved. Same-server URLs become relative so they stay in
// this SPA; external Tiled URLs stay absolute. Non-Tiled URIs (e.g. a Wikidata
// reference) are returned unchanged.
export function dataHref(uri: string): string {
  try {
    const url = new URL(uri, window.location.origin);
    const match = url.pathname.match(/^(.*?)\/api\/v1\/metadata\/(.+)$/);
    if (match) {
      const prefix = match[1]; // server root path, usually empty
      const path = `${prefix}/ui/browse/${match[2]}${url.search}${url.hash}`;
      return isSameServer(url) ? path : `${url.origin}${path}`;
    }
  } catch {
    // Not a parseable URL; fall through and link to it verbatim.
  }
  return uri;
}

// An entity is "external" when it carries a URI that resolves to a different
// origin than the running UI (e.g. a dataset on another Tiled server).
export function isExternalEntity(entity: GraphEntity): boolean {
  if (!entity.uri) return false;
  try {
    return !isSameServer(new URL(entity.uri, window.location.origin));
  } catch {
    return false;
  }
}

// The fill color for an entity's node. Datasets hosted on another Tiled server
// get a distinct shade of blue; every other node is colored by its type.
export function nodeColor(entity: GraphEntity): string {
  if (entity.entityType === "dataset" && isExternalEntity(entity)) {
    return EXTERNAL_DATASET_COLOR;
  }
  return colorFor(entity.entityType);
}

type NodeData = { label: string; entity: GraphEntity };
type EdgeData = { link: GraphLink };

export function buildElements(data: GraphData): { nodes: Node[]; edges: Edge[] } {
  const known = new Set(data.entities.map((e) => e.id));
  const nodes: Node[] = data.entities.map((entity) => ({
    id: entity.id,
    position: { x: 0, y: 0 },
    data: { label: entity.name, entity },
    style: {
      background: nodeColor(entity),
      color: "#fff",
      border: "none",
      borderRadius: 6,
      fontSize: 12,
      width: 180,
    },
  }));
  const edges: Edge[] = data.links
    // Guard against links that reference an entity we cannot see (access control).
    .filter((link) => known.has(link.subjectId) && known.has(link.objectId))
    .map((link) => ({
      id: link.id,
      source: link.subjectId,
      target: link.objectId,
      label: link.predicate,
      data: { link },
      labelStyle: { fontSize: 10, fill: "#555" },
      style: { stroke: "#90a4ae" },
      markerEnd: { type: MarkerType.ArrowClosed, color: "#90a4ae" },
    }));
  return { nodes, edges };
}

type Selection =
  | { kind: "node"; entity: GraphEntity }
  | { kind: "edge"; link: GraphLink }
  | null;

function PropertiesBlock({ value }: { value: Record<string, unknown> | null }) {
  if (!value || Object.keys(value).length === 0) {
    return (
      <Typography variant="body2" color="text.secondary">
        (none)
      </Typography>
    );
  }
  return (
    <Box
      component="pre"
      sx={{
        m: 0,
        p: 1,
        bgcolor: "grey.100",
        borderRadius: 1,
        fontSize: 12,
        overflowX: "auto",
        whiteSpace: "pre-wrap",
        wordBreak: "break-word",
      }}
    >
      {JSON.stringify(value, null, 2)}
    </Box>
  );
}

// A compact link to the entity's data, shown beside the entity name. The full
// destination URL appears on hover; clicking opens it in a new tab. Tiled
// datasets open their UI browse page (the actual array/table/container), not
// the metadata document. Renders nothing when the entity has no URI.
function UriPictogram({ uri }: { uri: string | null }) {
  if (!uri) return null;
  const href = dataHref(uri);
  return (
    <Tooltip title={href}>
      <IconButton
        component="a"
        href={href}
        target="_blank"
        rel="noopener"
        size="small"
        aria-label={`Open ${href}`}
      >
        <OpenInNewIcon fontSize="small" />
      </IconButton>
    </Tooltip>
  );
}

function DetailPanel({ selection }: { selection: Selection }) {
  if (selection === null) {
    return (
      <Typography variant="body2" color="text.secondary">
        Select a node or edge to see its details.
      </Typography>
    );
  }
  if (selection.kind === "node") {
    const e = selection.entity;
    return (
      <Stack spacing={1.5}>
        <Box>
          <Stack direction="row" alignItems="center" spacing={0.5}>
            <Typography variant="h6">{e.name}</Typography>
            <UriPictogram uri={e.uri} />
          </Stack>
          <Chip
            size="small"
            label={e.entityType}
            sx={{ bgcolor: nodeColor(e), color: "#fff" }}
          />
        </Box>
        <Box>
          <Typography variant="subtitle2">Properties</Typography>
          <PropertiesBlock value={e.properties} />
        </Box>
        <Typography variant="caption" color="text.secondary">
          id: {e.id}
        </Typography>
      </Stack>
    );
  }
  const l = selection.link;
  return (
    <Stack spacing={1.5}>
      <Box>
        <Typography variant="h6">Link</Typography>
        <Chip size="small" label={l.predicate} />
      </Box>
      <Box>
        <Typography variant="subtitle2">Properties</Typography>
        <PropertiesBlock value={l.properties} />
      </Box>
      <Typography variant="caption" color="text.secondary">
        id: {l.id}
      </Typography>
    </Stack>
  );
}

function Legend({
  types,
  hidden,
  onToggle,
}: {
  types: string[];
  hidden: Set<string>;
  onToggle: (entityType: string) => void;
}) {
  return (
    <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap alignItems="center">
      {types.map((type) => {
        const off = hidden.has(type);
        return (
          <Tooltip key={type} title={off ? `Show ${type}` : `Hide ${type}`}>
            <Chip
              size="small"
              label={type}
              clickable
              aria-pressed={!off}
              onClick={() => onToggle(type)}
              variant={off ? "outlined" : "filled"}
              sx={{
                bgcolor: off ? "transparent" : colorFor(type),
                color: off ? "text.disabled" : "#fff",
                borderColor: colorFor(type),
                textDecoration: off ? "line-through" : "none",
                opacity: off ? 0.7 : 1,
              }}
            />
          </Tooltip>
        );
      })}
    </Stack>
  );
}

const GraphExplorer = () => {
  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);
  const [rawData, setRawData] = useState<GraphData | null>(null);
  const [direction, setDirection] = useState<LayoutDirection>("LR");
  const [selection, setSelection] = useState<Selection>(null);
  const [hiddenTypes, setHiddenTypes] = useState<Set<string>>(
    () => new Set(DEFAULT_HIDDEN_TYPES),
  );
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);
    setError(null);
    fetchFullGraph(controller.signal)
      .then((data) => {
        setRawData(data);
        setLoading(false);
      })
      .catch((err) => {
        if (controller.signal.aborted) return;
        setError(err.message ?? String(err));
        setLoading(false);
      });
    return () => controller.abort();
  }, []);

  // Hide whole entity-type groups by dropping their nodes; buildElements then
  // drops any edge that referenced a now-hidden node.
  const visibleData = useMemo<GraphData | null>(() => {
    if (!rawData) return null;
    return {
      entities: rawData.entities.filter((e) => !hiddenTypes.has(e.entityType)),
      links: rawData.links,
    };
  }, [rawData, hiddenTypes]);

  const legendTypes = useMemo(
    () => (rawData ? entityTypesIn(rawData) : []),
    [rawData],
  );

  const toggleType = useCallback((entityType: string) => {
    setHiddenTypes((prev) => {
      const next = new Set(prev);
      if (next.has(entityType)) next.delete(entityType);
      else next.add(entityType);
      return next;
    });
  }, []);

  useEffect(() => {
    if (!visibleData) return;
    const { nodes: builtNodes, edges: builtEdges } = buildElements(visibleData);
    setNodes(layoutGraph(builtNodes, builtEdges, direction));
    setEdges(builtEdges);
  }, [visibleData, direction, setNodes, setEdges]);

  const onNodeClick = useCallback((_: unknown, node: Node) => {
    setSelection({ kind: "node", entity: (node.data as NodeData).entity });
  }, []);
  const onEdgeClick = useCallback((_: unknown, edge: Edge) => {
    setSelection({ kind: "edge", link: (edge.data as EdgeData).link });
  }, []);

  const miniMapNodeColor = useCallback(
    (node: Node) => nodeColor((node.data as NodeData).entity),
    [],
  );

  const isEmpty = useMemo(
    () => rawData !== null && rawData.entities.length === 0,
    [rawData],
  );

  if (loading) {
    return <Skeleton variant="rectangular" height={600} />;
  }
  if (error) {
    return <Alert severity="error">Failed to load graph: {error}</Alert>;
  }

  return (
    <Stack spacing={2}>
      <Box
        sx={{
          display: "flex",
          alignItems: "center",
          flexWrap: "wrap",
          gap: 2,
        }}
      >
        <Legend
          types={legendTypes}
          hidden={hiddenTypes}
          onToggle={toggleType}
        />
        <Box sx={{ flexGrow: 1 }} />
        <ToggleButtonGroup
          size="small"
          exclusive
          value={direction}
          onChange={(_, value) => value && setDirection(value)}
        >
          <ToggleButton value="LR">Left → Right</ToggleButton>
          <ToggleButton value="TB">Top → Bottom</ToggleButton>
        </ToggleButtonGroup>
      </Box>

      {isEmpty && (
        <Alert severity="info">
          No entities are visible. The graph may be empty, or your account may
          lack read access.
        </Alert>
      )}

      <Box
        sx={{
          display: "flex",
          gap: 2,
          flexDirection: { xs: "column", md: "row" },
        }}
      >
        <Paper
          variant="outlined"
          sx={{ height: 600, flexGrow: 1, minWidth: 0 }}
        >
          <ReactFlow
            nodes={nodes}
            edges={edges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onNodeClick={onNodeClick}
            onEdgeClick={onEdgeClick}
            fitView
            minZoom={0.1}
            proOptions={{ hideAttribution: true }}
          >
            <Background />
            <Controls />
            <MiniMap nodeColor={miniMapNodeColor} pannable zoomable />
          </ReactFlow>
        </Paper>
        <Paper
          variant="outlined"
          sx={{ width: { xs: "100%", md: 320 }, flexShrink: 0, p: 2 }}
        >
          <DetailPanel selection={selection} />
        </Paper>
      </Box>
    </Stack>
  );
};

export default GraphExplorer;
