import Contents, { Column, Spec } from "../components/contents";
import { useEffect, useState } from "react";

import Container from "@mui/material/Container";
import Typography from "@mui/material/Typography";
import { components } from "../openapi_schemas";
import { search } from "../client";

interface NodeContentsProps {
  segments: string[];
  columns: Column[];
  defaultColumns: string[];
  specs: string[];
}

const specs =
  (JSON.parse(sessionStorage.getItem("config") as string).specs as Spec[]) ||
  [];

const NodeContents: React.FunctionComponent<NodeContentsProps> = (props) => {
  const [items, setItems] = useState<
    components["schemas"]["Resource_NodeAttributes__dict__dict_"][]
  >([]);
  useEffect(() => {
    const controller = new AbortController();
    var selectMetadata: string | null;
    var fields: string[];
    if (props.columns.length === 0) {
      // No configuration on which columns to show. Fetch only the ID.
      fields = [];
      selectMetadata = null;
    } else {
      fields = ["metadata"];
      selectMetadata =
        "{" +
        props.columns
          .map((column) => {
            return `${column.field}:${column.select_metadata}`;
          })
          .join(",") +
        "}";
    }
    async function loadData() {
      var items = await search(
        props.segments,
        controller.signal,
        fields,
        selectMetadata
      );
      setItems(items);
    }
    loadData();
    return () => {
      controller.abort();
    };
  }, [props.segments]);
  return (
    <Contents
      items={items}
      specs={props.specs}
      columns={props.columns}
      defaultColumns={props.defaultColumns}
    />
  );
};

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

export { NodeOverview, NodeContents };
