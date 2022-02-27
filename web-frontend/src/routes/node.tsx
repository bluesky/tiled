import * as React from "react";

import { useEffect, useState } from "react";

import { ArrayOverview } from "../components/overview-array";
import Box from "@mui/material/Box";
import { DataFrameOverview } from "../components/overview-dataframe";
import DownloadArray from "../components/download-array";
import DownloadDataFrame from "../components/download-dataframe";
import DownloadDataset from "../components/download-xarray-dataset";
import JSONViewer from "../components/json-viewer";
import MetadataView from "../components/metadata-view";
import NodeBreadcrumbs from "../components/node-breadcrumbs";
import { NodeOverview } from "../components/overview-generic-node";
import Paper from "@mui/material/Paper";
import PropTypes from "prop-types";
import Skeleton from "@mui/material/Skeleton";
import Tab from "@mui/material/Tab";
import Tabs from "@mui/material/Tabs";
import Typography from "@mui/material/Typography";
import { XarrayDataArrayOverview } from "../components/overview-xarray-data-array";
import { XarrayDatasetOverview } from "../components/overview-xarray-dataset";
import { components } from "../openapi_schemas";
import { metadata } from "../client";
import { useParams } from "react-router-dom";

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

const DownloadDispatch: React.FunctionComponent<DispatchProps> = (props) => {
  // Dispatch to a specific overview component based on the structure family.
  // In the future we will extend this to consider 'specs' as well.
  if (props.item !== undefined) {
    const attributes = props.item.data!.attributes!;
    const structureFamily = attributes.structure_family;
    switch (structureFamily) {
      case "node":
        return (
          <DownloadArray
            name={props.item.data!.id}
            structureFamily={structureFamily}
            macrostructure={null}
            specs={attributes.specs as string[]}
            link={props.item.data!.links!.full! as string}
          />
        );
      case "array":
        return (
          <DownloadArray
            name={props.item.data!.id}
            structureFamily={structureFamily}
            macrostructure={attributes.structure!.macro!}
            specs={attributes.specs as string[]}
            link={props.item.data!.links!.full! as string}
          />
        );
      case "dataframe":
        return (
          <DownloadDataFrame
            name={props.item.data!.id}
            structureFamily={structureFamily}
            macrostructure={attributes.structure!.macro!}
            specs={attributes.specs as string[]}
            full_link={props.item.data!.links!.full! as string}
            partition_link={props.item.data!.links!.partition! as string}
          />
        );
      case "xarray_data_array":
        // special case: handle this like an array
        const structure = attributes!.structure! as any;
        const macrostructure = structure.macro!.variable!.macro!;
        return (
          <DownloadArray
            name={props.item.data!.id}
            structureFamily="array"
            macrostructure={macrostructure}
            specs={attributes.specs as string[]}
            link={props.item.data!.links!.full_variable! as string}
          />
        );
      case "xarray_dataset":
        return (
          <DownloadDataset
            name={props.item.data!.id}
            structureFamily={structureFamily}
            macrostructure={attributes.structure!.macro!}
            specs={attributes.specs as string[]}
            link={props.item.data!.links!.full_dataset! as string}
          />
        );
      default:
        return <div>Unknown structure family "{structureFamily}"</div>;
    }
  }
  return <Skeleton variant="rectangular" />;
};

const OverviewDispatch: React.FunctionComponent<DispatchProps> = (props) => {
  // Dispatch to a specific overview component based on the structure family.
  // In the future we will extend this to consider 'specs' as well.
  if (props.item !== undefined) {
    const structureFamily = props.item!.data!.attributes!.structure_family;
    switch (structureFamily) {
      case "node":
        return <NodeOverview segments={props.segments} item={props.item} />;
      case "array":
        return (
          <ArrayOverview
            segments={props.segments}
            item={props.item}
            structure={props.item.data!.attributes!.structure!}
          />
        );
      case "dataframe":
        return (
          <DataFrameOverview segments={props.segments} item={props.item} />
        );
      case "xarray_data_array":
        return (
          <XarrayDataArrayOverview
            segments={props.segments}
            item={props.item}
            structure={
              props.item.data!.attributes!.structure!.macro!
                .variable! as components["schemas"]["Structure"]
            }
          />
        );
      case "xarray_dataset":
        return (
          <XarrayDatasetOverview segments={props.segments} item={props.item} />
        );
      default:
        return <div>Unknown structure family "{structureFamily}"</div>;
    }
  }
  return <Skeleton variant="rectangular" />;
};

function Node() {
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

const NodeTabs: React.FunctionComponent<IProps> = (props) => {
  const [tabValue, setTabValue] = useState(0);
  const handleTabChange = (event: React.ChangeEvent<any>, newValue: number) => {
    setTabValue(newValue);
  };
  const [item, setItem] =
    useState<
      components["schemas"]["Response_Resource_NodeAttributes__dict__dict___dict__dict_"]
    >();
  useEffect(() => {
    setItem(undefined);
    const controller = new AbortController();
    async function loadData() {
      // Request all the attributes.
      var result = await metadata(props.segments, controller.signal, [
        "structure_family",
        "structure.macro",
        "structure.micro",
        "specs",
      ]);
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
      var result = await metadata(props.segments, controller.signal, [
        "structure_family",
        "structure.macro",
        "structure.micro",
        "specs",
        "metadata",
        "sorting",
        "count",
      ]);
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
          <OverviewDispatch segments={props.segments} item={item} />
        </Paper>
      </TabPanel>
      <TabPanel value={tabValue} index={1}>
        <DownloadDispatch segments={props.segments} item={item} />
      </TabPanel>
      <TabPanel value={tabValue} index={2}>
        <MetadataView json={fullItem} />
      </TabPanel>
      <TabPanel value={tabValue} index={3}>
        <JSONViewer json={fullItem} />
      </TabPanel>
    </Box>
  );
};

export default Node;
