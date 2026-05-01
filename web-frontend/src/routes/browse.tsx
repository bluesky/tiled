import * as React from "react";

import { Suspense, lazy, useContext, useEffect, useState } from "react";

import Box from "@mui/material/Box";
import ErrorBoundary from "../components/error-boundary/error-boundary";
import NodeBreadcrumbs from "../components/node-breadcrumbs/node-breadcrumbs";
import Paper from "@mui/material/Paper";
import PropTypes from "prop-types";
import Skeleton from "@mui/material/Skeleton";
import Tab from "@mui/material/Tab";
import Tabs from "@mui/material/Tabs";
import Typography from "@mui/material/Typography";
import { components } from "../openapi_schemas";
import { metadata } from "../client";
import { SettingsContext } from "../context/settings";
import { useParams } from "react-router-dom";

const ArrayOverview = lazy(
  () => import("../components/overview-array/overview-array"),
);
const TableOverview = lazy(
  () => import("../components/overview-table/overview-table"),
);
const DownloadArray = lazy(
  () => import("../components/download-array/download-array"),
);
const DownloadTable = lazy(
  () => import("../components/download-table/download-table"),
);
const DownloadNode = lazy(
  () => import("../components/download-node/download-node"),
);
const JSONViewer = lazy(() => import("../components/json-viewer/json-viewer"));
const MetadataView = lazy(
  () => import("../components/metadata-view/metadata-view"),
);
const NodeOverview = lazy(
  () => import("../components/overview-generic-node/overview-generic-node"),
);

// Cache of loaded script URLs to avoid duplicate <script> tags.
// Once a script has loaded and its IIFE has executed (registering the
// component on window.__TILED_SPEC_VIEWS__), there is no need to load
// it again — even if the <script> element is removed from the DOM.
const loadedScripts = new Set<string>();

function loadScript(url: string): Promise<void> {
  if (loadedScripts.has(url)) return Promise.resolve();
  return new Promise((resolve, reject) => {
    const script = document.createElement("script");
    script.src = url;
    script.async = true;
    script.onload = () => {
      loadedScripts.add(url);
      resolve();
    };
    script.onerror = () => {
      // Remove failed script so a retry is possible.
      script.remove();
      reject(new Error(`Failed to load spec view: ${url}`));
    };
    document.head.appendChild(script);
  });
}

/**
 * Dynamically loads and renders an external spec view component.
 *
 * The external JS bundle (IIFE) is expected to register its component at:
 *   window.__TILED_SPEC_VIEWS__[specName]
 *
 * The component receives the same props as built-in overview components:
 *   { segments, item }
 */
function DynamicSpecView({
  specName,
  url,
  segments,
  item,
}: {
  specName: string;
  url: string;
  segments: string[];
  item: any;
}) {
  const [Component, setComponent] = useState<React.ComponentType<any> | null>(
    null,
  );
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    loadScript(url)
      .then(() => {
        if (cancelled) return;
        const registry = (window as any).__TILED_SPEC_VIEWS__;
        const comp = registry?.[specName];
        if (comp) {
          setComponent(() => comp);
        } else {
          setError(
            `Spec view "${specName}" not found in window.__TILED_SPEC_VIEWS__ after loading ${url}`,
          );
        }
      })
      .catch((err) => {
        if (!cancelled) setError(err.message);
      });
    return () => {
      cancelled = true;
    };
  }, [specName, url]);

  if (error) return <div>Error loading spec view: {error}</div>;
  if (!Component) return <Skeleton variant="rectangular" />;
  return <Component segments={segments} item={item} />;
}

interface TabPanelProps {
  children?: React.ReactNode;
  index: any;
  value: any;
}

function TabPanel(props: TabPanelProps) {
  const { children, value, index, ...other } = props;

  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`simple-tabpanel-${index}`}
      aria-labelledby={`simple-tab-${index}`}
      {...other}
    >
      {value === index && <Box sx={{ p: 3 }}>{children}</Box>}
    </div>
  );
}

TabPanel.propTypes = {
  children: PropTypes.node,
  index: PropTypes.number.isRequired,
  value: PropTypes.number.isRequired,
};

function a11yProps(index: number) {
  return {
    id: `simple-tab-${index}`,
    "aria-controls": `simple-tabpanel-${index}`,
  };
}

interface IProps {
  segments: string[];
}

interface DispatchProps {
  segments: string[];
  item:
    | components["schemas"]["Response_Resource_NodeAttributes__dict__dict___dict__dict_"]
    | undefined;
}

export const DownloadDispatch: React.FunctionComponent<DispatchProps> = (
  props,
) => {
  // Dispatch to a specific overview component based on the structure family.
  // In the future we will extend this to consider 'specs' as well.
  if (props.item !== undefined) {
    const attributes = props.item.data!.attributes!;
    const structureFamily = attributes.structure_family;
    switch (structureFamily) {
      case "container":
        return (
          <DownloadNode
            name={props.item.data!.id}
            structureFamily={structureFamily}
            specs={attributes.specs as string[]}
            link={props.item.data!.links!.full! as string}
          />
        );
      case "array":
        return (
          <DownloadArray
            name={props.item.data!.id}
            structureFamily={structureFamily}
            structure={attributes.structure!}
            specs={attributes.specs as string[]}
            link={props.item.data!.links!.full! as string}
          />
        );
      case "table":
        return (
          <DownloadTable
            name={props.item.data!.id}
            structureFamily={structureFamily}
            structure={attributes.structure!}
            specs={attributes.specs as string[]}
            full_link={props.item.data!.links!.full! as string}
            partition_link={props.item.data!.links!.partition! as string}
          />
        );
      default:
        return <div>Unknown structure family "{structureFamily}"</div>;
    }
  }
  return <Skeleton variant="rectangular" />;
};

export const OverviewDispatch: React.FunctionComponent<DispatchProps> = (
  props,
) => {
  const settings = useContext(SettingsContext);

  // Dispatch to a specific overview component based on the structure family.
  // If spec_views are configured, check for a matching spec first.
  if (props.item !== undefined) {
    const attributes = props.item!.data!.attributes!;

    // Check for external spec_views (plugin components loaded at runtime)
    if (settings.spec_views && settings.spec_views.length > 0) {
      const specs = (attributes.specs || []) as any[];
      const specNames = specs.map((s: any) =>
        typeof s === "string" ? s : s.name || "",
      );
      for (const sv of settings.spec_views) {
        if (specNames.includes(sv.spec)) {
          return (
            <ErrorBoundary>
              <DynamicSpecView
                specName={sv.spec}
                url={sv.url}
                segments={props.segments}
                item={props.item}
              />
            </ErrorBoundary>
          );
        }
      }
    }

    const structureFamily = attributes.structure_family;
    switch (structureFamily) {
      case "container":
        return <NodeOverview segments={props.segments} item={props.item} />;
      case "array":
        return (
          <ArrayOverview
            segments={props.segments}
            item={props.item}
            link={props.item.data!.links!.full as string}
            structure={props.item.data!.attributes!.structure!}
          />
        );
      case "table":
        return <TableOverview segments={props.segments} item={props.item} />;
      default:
        return <div>Unknown structure family "{structureFamily}"</div>;
    }
  }
  return <Skeleton variant="rectangular" />;
};

function Browse() {
  // Extract from path from react-router.
  const params = useParams<{ "*": string }>();
  // Transform "/a/b/c" to ["a", "b", "c"].
  const segments = (params["*"] || "").split("/").filter(function (segment) {
    return segment;
  });

  if (segments !== undefined) {
    return (
      <Box sx={{ width: "100%" }}>
        <NodeBreadcrumbs segments={segments} />
        <NodeTabs segments={segments} />
      </Box>
    );
  } else {
    return <Skeleton variant="text" />;
  }
}

export const NodeTabs: React.FunctionComponent<IProps> = (props) => {
  const settings = useContext(SettingsContext);
  const [tabValue, setTabValue] = useState(0);
  const handleTabChange = (event: React.ChangeEvent<any>, newValue: number) => {
    setTabValue(newValue);
  };
  const [item, setItem] =
    useState<
      components["schemas"]["Response_Resource_NodeAttributes__dict__dict___dict__dict_"]
    >();
  useEffect(() => {
    setTabValue(0);
  }, [props.segments]);
  useEffect(() => {
    setItem(undefined);
    const controller = new AbortController();
    async function loadData() {
      // Request all the attributes.
      const result = await metadata(
        settings.api_url,
        props.segments,
        controller.signal,
        ["structure_family", "structure", "specs"],
      );
      if (result !== undefined) {
        setItem(result);
      }
    }
    loadData();
    return () => {
      controller.abort();
    };
  }, [props.segments]);
  const [fullItem, setFullItem] =
    useState<
      components["schemas"]["Response_Resource_NodeAttributes__dict__dict___dict__dict_"]
    >();
  useEffect(() => {
    const controller = new AbortController();
    async function loadData() {
      // Request all the attributes.
      const result = await metadata(
        settings.api_url,
        props.segments,
        controller.signal,
        [
          "structure_family",
          "structure",
          "specs",
          "metadata",
          "sorting",
          "count",
        ],
      );
      if (result !== undefined) {
        setFullItem(result);
      }
    }
    loadData();
    return () => {
      controller.abort();
    };
  }, [props.segments]);
  return (
    <Box sx={{ width: "100%" }}>
      <Box sx={{ borderBottom: 1, borderColor: "divider" }}>
        <Tabs
          value={tabValue}
          onChange={handleTabChange}
          aria-label="basic tabs example"
        >
          <Tab label="View" {...a11yProps(0)} />
          <Tab label="Download" {...a11yProps(1)} />
          <Tab label="Metadata" {...a11yProps(2)} />
          <Tab label="Detail" {...a11yProps(3)} />
        </Tabs>
      </Box>
      <TabPanel value={tabValue} index={0}>
        <Typography variant="h4" component="h1" gutterBottom>
          {props.segments.length > 0
            ? props.segments[props.segments.length - 1]
            : ""}
        </Typography>
        <Paper elevation={3} sx={{ px: 3, py: 3 }}>
          <ErrorBoundary>
            <Suspense fallback={<Skeleton variant="rectangular" />}>
              <OverviewDispatch segments={props.segments} item={item} />
            </Suspense>
          </ErrorBoundary>
        </Paper>
      </TabPanel>
      <TabPanel value={tabValue} index={1}>
        <ErrorBoundary>
          <Suspense fallback={<Skeleton variant="rectangular" />}>
            <DownloadDispatch segments={props.segments} item={item} />
          </Suspense>
        </ErrorBoundary>
      </TabPanel>
      <TabPanel value={tabValue} index={2}>
        <ErrorBoundary>
          <Suspense fallback={<Skeleton variant="rectangular" />}>
            <MetadataView json={fullItem} />
          </Suspense>
        </ErrorBoundary>
      </TabPanel>
      <TabPanel value={tabValue} index={3}>
        <ErrorBoundary>
          <Suspense fallback={<Skeleton variant="rectangular" />}>
            <JSONViewer json={fullItem} />
          </Suspense>
        </ErrorBoundary>
      </TabPanel>
    </Box>
  );
};

export default Browse;
