import Contents, { Column } from "../components/contents";
import { useEffect, useState } from "react";

import { components } from "../openapi_schemas";
import { search } from "../client";

interface NodeContentsProps {
  segments: string[];
  columns: Column[];
  defaultColumns: string[];
  specs: string[];
}

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
  }, [props.columns, props.segments]);
  return (
    <Contents
      items={items}
      specs={props.specs}
      columns={props.columns}
      defaultColumns={props.defaultColumns}
    />
  );
};

export default NodeContents;
