import ArrayOverview from "./overview-array";
import Container from "@mui/material/Container";
import Divider from "@mui/material/Divider";
import NodeContents from "./node-contents";
import Typography from "@mui/material/Typography";
import { components } from "../openapi_schemas";

interface IProps {
  segments: string[];
  item: any;
  link: string;
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
        link={props.item.data!.links!.full_variable as string}
        structure={props.structure}
      />
      <Divider sx={{ mb: 3 }} />
      <Typography id="table-title" variant="h6" component="h2">
        Coordinates
      </Typography>
      <NodeContents
        segments={props.segments.concat(["coords"])}
        specs={props.item!.data!.attributes!.specs}
        columns={[]}
        defaultColumns={["id"]}
      />
    </Container>
  );
};

export default XarrayDataArrayOverview;
