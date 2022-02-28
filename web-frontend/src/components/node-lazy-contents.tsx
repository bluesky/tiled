import {
  DataGrid,
  GridRowModel,
  GridRowParams,
  GridToolbarColumnsButton,
  GridToolbarContainer,
  GridToolbarDensitySelector,
} from "@mui/x-data-grid";
import { useEffect, useState } from "react";

import Box from "@mui/material/Box";
import Container from "@mui/material/Container";
import { components } from "../openapi_schemas";
import { search } from "../client";
import { useNavigate } from "react-router-dom";

interface Column {
  header: string;
  field: string;
  select_metadata: string;
}

interface Spec {
  spec: string;
  columns: Column[];
  default_columns: string[];
}

interface IProps {
  rowCount: number;
  specs: string[];
  columns: Column[];
  defaultColumns: string[];
}

function CustomToolbar() {
  return (
    <GridToolbarContainer>
      <GridToolbarColumnsButton />
      <GridToolbarDensitySelector />
    </GridToolbarContainer>
  );
}

interface RowsState {
  page: number;
  pageSize: number;
  rows: GridRowModel[];
  loading: boolean;
}

interface NodeLazyContentsProps {
  segments: string[];
  columns: Column[];
  defaultColumns: string[];
  specs: string[];
}

const DEFAULT_PAGE_SIZE = 10;

const NodeLazyContents: React.FunctionComponent<NodeLazyContentsProps> = (
  props
) => {
  let navigate = useNavigate();
  const gridColumns = [
    {
      field: "id",
      headerName: "ID",
      flex: 1,
      hide: !props.defaultColumns.includes("id"),
    },
  ];
  props.columns.map((column) =>
    gridColumns.push({
      field: column.field,
      headerName: column.header,
      flex: 1,
      hide: !props.defaultColumns.includes(column.field),
    })
  );
  const [rowsState, setRowsState] = useState<RowsState>({
    page: 0,
    pageSize: DEFAULT_PAGE_SIZE,
    rows: [],
    loading: false,
  });
  type IdsToAncestors = { [key: string]: string[] };
  const [idsToAncestors, setIdsToAncestors] = useState<IdsToAncestors>({});
  const [rowCount, setRowCount] = useState<number>(0);
  async function loadItems(): Promise<
    components["schemas"]["Resource_NodeAttributes__dict__dict_"][]
  > {
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
      selectMetadata,
      rowsState.pageSize * rowsState.page,
      rowsState.pageSize
    );
    setRowCount(data.meta!.count! as number);
    const items = data.data;
    return items!;
  }
  useEffect(() => {
    console.log("useEffect");
    let active = true;

    (async () => {
      setRowsState((prev) => ({ ...prev, loading: true }));
      const newItems = await loadItems();
      console.log("newItems", newItems);
      var idsToAncestors: IdsToAncestors = {};
      newItems.map(
        (
          item: components["schemas"]["Resource_NodeAttributes__dict__dict_"]
        ) => {
          idsToAncestors[item.id as string] = item.attributes.ancestors;
          return null;
        }
      );
      const newRows = newItems.map(
        (
          item: components["schemas"]["Resource_NodeAttributes__dict__dict_"]
        ) => {
          const row: { [key: string]: any } = {};
          row.id = item.id;
          props.columns.map((column) => {
            row[column.field] = item.attributes!.metadata![column.field];
            return null;
          });
          return row;
        }
      );

      if (!active) {
        console.log("return early");
        return;
      }

      // TODO Synchronize these. (Clear rows first?)
      console.log("set rows");
      setIdsToAncestors(idsToAncestors);
      setRowsState((prev) => ({ ...prev, loading: false, rows: newRows }));
    })();

    return () => {
      active = false;
    };
  }, [rowsState.page, rowsState.pageSize]);

  return (
    <Box sx={{ my: 4 }}>
      <Container maxWidth="lg">
        <DataGrid
          columns={gridColumns}
          pagination
          rowCount={rowCount}
          {...rowsState}
          paginationMode="server"
          rowsPerPageOptions={[10, 30, 100]}
          onPageChange={(page) => setRowsState((prev) => ({ ...prev, page }))}
          onPageSizeChange={(pageSize) => {
            setRowsState((prev) => ({ ...prev, pageSize }));
          }}
          onRowClick={(params: GridRowParams) => {
            navigate(
              `/browse${idsToAncestors[params.id]
                .map(function (ancestor: string) {
                  return "/" + ancestor;
                })
                .join("")}/${params.id}`
            );
          }}
          components={{
            Toolbar: CustomToolbar,
          }}
          disableColumnFilter
          autoHeight
        />
      </Container>
    </Box>
  );
};

export default NodeLazyContents;
