import Container from "@mui/material/Container";
import Contents from "../components/contents";
import Typography from "@mui/material/Typography";

interface IProps {
  segments: string[];
  item: any;
}

const NodeOverview: React.FunctionComponent<IProps> = (props) => {
  return (
    <Container maxWidth="lg">
      <Typography id="table-title" variant="h6" component="h2">
        Contents
      </Typography>
      <Contents segments={props.segments} />
    </Container>
  );
};

export { NodeOverview };
