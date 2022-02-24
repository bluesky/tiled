import { useEffect, useState } from "react";

import Container from "@mui/material/Container";
import Contents from "../components/contents";
import Typography from "@mui/material/Typography";
import { components } from "../openapi_schemas";
import { search } from "../client";

interface NodeContentsProps {
  segments: string[];
}

const NodeContents: React.FunctionComponent<NodeContentsProps> = (props) => {
  const [items, setItems] = useState<
    components["schemas"]["Resource_NodeAttributes__dict__dict_"][]
  >([]);
  useEffect(() => {
    const controller = new AbortController();
    async function loadData() {
      var items = await search(props.segments, controller.signal);
      setItems(items);
    }
    loadData();
    return () => {
      controller.abort();
    };
  }, [props.segments]);
  return <Contents items={items} />;
};

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
      <NodeContents segments={props.segments} />
    </Container>
  );
};

export { NodeOverview };
