import Container from "@mui/material/Container";
import { NodeContents } from "./overview-generic-node";
import Typography from "@mui/material/Typography";

interface IProps {
  segments: string[];
  item: any;
}

const XarrayDatasetOverview: React.FunctionComponent<IProps> = (props) => {
  return (
    <Container maxWidth="lg">
      <Typography id="table-title" variant="h6" component="h2">
        Data Variables
      </Typography>
      <NodeContents segments={props.segments.concat(["data_vars"])} specs={props.item.data!.attributes!.specs!} columns={[]} />
      <Typography id="table-title" variant="h6" component="h2">
        Coordinates
      </Typography>
      <NodeContents segments={props.segments.concat(["coords"])} specs={props.item.data!.attributes!.specs!} columns={[]} />
    </Container>
  );
};

export { XarrayDatasetOverview };
