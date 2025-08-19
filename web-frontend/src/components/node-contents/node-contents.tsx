import Contents, { Column } from "../contents/contents";
import { useContext, useEffect, useState } from "react";

import { components } from "../../openapi_schemas";
import { search } from "../../client";
import { SettingsContext } from "../../context/settings";

interface NodeContentsProps {
  segments: string[];
  columns: Column[];
  defaultColumns: string[];
  specs: string[];
}

const NodeContents: React.FunctionComponent<NodeContentsProps> = (props) => {
  const settings = useContext(SettingsContext);
  const [items, setItems] = useState<
    components["schemas"]["Resource_NodeAttributes__dict__dict_"][]
  >([]);
  useEffect(() => {
    const controller = new AbortController();
    let selectMetadata: string | null;
    let fields: string[];
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
      const data = await search(
        settings.api_url,
        props.segments,
        controller.signal,
        fields,
        selectMetadata,
      );
      setItems(data!.data!);
    }
    loadData();
    return () => {
      controller.abort();
    };
  }, [props.columns, props.segments]);
  return (
    <Contents
      items={items!}
      specs={props.specs}
      columns={props.columns}
      defaultColumns={props.defaultColumns}
    />
  );
};

export default NodeContents;
