import { Suspense, lazy } from "react";

import Alert from "@mui/material/Alert";
import Box from "@mui/material/Box";
import Skeleton from "@mui/material/Skeleton";

import ErrorBoundary from "../components/error-boundary/error-boundary";

const GraphExplorer = lazy(
  () => import("../components/graph-explorer/graph-explorer"),
);

function GraphPage() {
  return (
    <Box sx={{ width: "100%", py: 2 }}>
      <Alert severity="info" sx={{ mb: 2 }}>
        This feature is experimental. It visualizes the entity/link graph served
        at <code>/api/graphql</code>. Click a node or edge to inspect it.
      </Alert>
      <ErrorBoundary>
        <Suspense fallback={<Skeleton variant="rectangular" height={600} />}>
          <GraphExplorer />
        </Suspense>
      </ErrorBoundary>
    </Box>
  );
}

export default GraphPage;
