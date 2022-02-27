import Contents, { Column, Spec } from "../components/contents";
import { useEffect, useState } from "react";

import Container from "@mui/material/Container";
import Typography from "@mui/material/Typography";
import { components } from "../openapi_schemas";
import { search } from "../client";

interface NodeContentsProps {
  segments: string[];
  columns: Column[];
  specs: string[];
}

const specs = JSON.parse(sessionStorage.getItem("config") as string).specs as Spec[] || []

const NodeContents: React.FunctionComponent<NodeContentsProps> = (props) => {
  const [items, setItems] = useState<
    components["schemas"]["Resource_NodeAttributes__dict__dict_"][]
  >([]);
  useEffect(() => {
    const controller = new AbortController();
    async function loadData() {
      var items = await search(props.segments, controller.signal, ["metadata"], []);
      setItems(items);
    }
    loadData();
    return () => {
      controller.abort();
    };
  }, [props.segments]);
  return <Contents items={items} specs={props.specs}/>;
};

interface IProps {
  segments: string[];
  item: any;
}

const NodeOverview: React.FunctionComponent<IProps> = (props) => {
  // Walk through the node's specs until we find one we recognize.
  const spec = specs.find(spec => props.item.data!.attributes!.specs.includes(spec.spec) );
  var columns: Column[];
  if ( spec === undefined ) {
    columns = []
  } else {
    columns = spec.columns
  }
  return (
    <Container maxWidth="lg">
      <Typography id="table-title" variant="h6" component="h2">
        Contents
      </Typography>
      <NodeContents segments={props.segments} specs={props.item.data!.attributes!.specs!} columns={columns} />
    </Container>
  );
};

export { NodeOverview, NodeContents };
