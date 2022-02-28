import { Column, Spec } from "./contents";
import { useEffect, useState } from "react";

import Container from "@mui/material/Container";
import NodeLazyContents from "./node-lazy-contents";
import { Skeleton } from "@mui/material";
import Typography from "@mui/material/Typography";
import { loadConfig } from "../config";

interface IProps {
  segments: string[];
  item: any;
}

const NodeOverview: React.FunctionComponent<IProps> = (props) => {
  const [specs, setSpecs] = useState<any>();

  useEffect(() => {
    const controller = new AbortController();
    async function loadSpecs() {
      const config = await loadConfig(controller.signal);
      const specs = config.specs;
      setSpecs(specs);
    }
    loadSpecs();
    return () => {
      controller.abort();
    };
  }, []);
  // Walk through the node's specs until we find one we recognize.
  if (specs === undefined) {
    return <Skeleton variant="rectangular" />;
  }
  const spec = specs.find((spec: Spec) =>
    props.item.data!.attributes!.specs.includes(spec.spec)
  );
  var columns: Column[];
  var defaultColumns: string[];
  if (spec === undefined) {
    columns = [];
    defaultColumns = ["id"];
  } else {
    columns = spec.columns;
    defaultColumns = spec.default_columns;
  }
  return (
    <Container maxWidth="lg">
      <Typography id="table-title" variant="h6" component="h2">
        Contents
      </Typography>
      <NodeLazyContents
        segments={props.segments}
        specs={props.item.data!.attributes!.specs!}
        columns={columns}
        defaultColumns={defaultColumns}
      />
    </Container>
  );
};

export default NodeOverview;
