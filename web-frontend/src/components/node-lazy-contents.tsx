import LazyContents, { Column } from "../components/lazy-contents";

import { components } from "../openapi_schemas";
import { search } from "../client";
import { useState } from "react";

interface NodeLazyContentsProps {
  segments: string[];
  columns: Column[];
  defaultColumns: string[];
  specs: string[];
}

const NodeLazyContents: React.FunctionComponent<NodeLazyContentsProps> = (
  props
) => {
  const [rowCount, setRowCount] = useState<number>(0);
  const [pageSize, setPageSize] = useState<number>(10);
  const [pageNumber, setPageNumber] = useState<number>(1);
  async function loadItems() : Promise<components["schemas"]["Resource_NodeAttributes__dict__dict_"][]> {
    var selectMetadata: string | null;
    var fields: string[];
    const controller = new AbortController();
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
    var data = await search(
      props.segments,
      controller.signal,
      fields,
      selectMetadata
    );
    setRowCount(data.meta!.count! as number);
    const items = data.data;
    return items!;
  }
  return (
    <LazyContents
      loadItems={loadItems}
      rowCount={rowCount}
      specs={props.specs}
      columns={props.columns}
      defaultColumns={props.defaultColumns}
      pageSize={pageSize}
      setPageSize={setPageSize}
    />
  );
};

export default NodeLazyContents;
