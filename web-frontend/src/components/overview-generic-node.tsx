import { Column, Spec } from "./contents";

import Container from "@mui/material/Container";
import NodeContents from "./node-contents";
import Typography from "@mui/material/Typography";
import { loadConfig } from "../config";

const specs = loadConfig().specs as Spec[] | [];

interface IProps {
  segments: string[];
  item: any;
}

const NodeOverview: React.FunctionComponent<IProps> = (props) => {
  // Walk through the node's specs until we find one we recognize.
  const spec = specs.find((spec) =>
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
      <NodeContents
        segments={props.segments}
        specs={props.item.data!.attributes!.specs!}
        columns={columns}
        defaultColumns={defaultColumns}
      />
    </Container>
  );
};

export default NodeOverview;
