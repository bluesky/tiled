import { ArrayOverview } from "./overview-array";
import Container from "@mui/material/Container";
import { NodeContents } from "./overview-generic-node";
import Typography from "@mui/material/Typography";
import { components } from "../openapi_schemas";

interface IProps {
  segments: string[];
  item: any;
  structure: components["schemas"]["Structure"];
}

//<NodeContents segments={props.segments.concat(["coords"])} />
const XarrayDataArrayOverview: React.FunctionComponent<IProps> = (props) => {
  return (
    <Container maxWidth="lg">
      <Typography id="table-title" variant="h6" component="h2">
        Variable
      </Typography>
      <ArrayOverview
        segments={props.segments}
        item={props.item}
        structure={props.structure}
      />
      <Typography id="table-title" variant="h6" component="h2">
        Coordinates
      </Typography>
      <NodeContents
        segments={props.segments.concat(["coords"])}
        specs={props.item!.data!.attributes!.specs}
        columns={[]}
      />
    </Container>
  );
};

export { XarrayDataArrayOverview };
