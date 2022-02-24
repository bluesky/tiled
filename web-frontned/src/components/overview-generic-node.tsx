import Container from "@mui/material/Container";
import Contents from "../components/contents";

interface IProps {
  segments: string[];
  item: any;
}

const NodeOverview: React.FunctionComponent<IProps> = (props) => {
  return (
    <Container maxWidth="lg">
      <Contents segments={props.segments} />
    </Container>
  );
};

export { NodeOverview };
