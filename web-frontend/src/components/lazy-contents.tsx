import * as React from "react";

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
  loadItems: any; // components["schemas"]["Resource_NodeAttributes__dict__dict_"][];
  rowCount: number;
  specs: string[];
  columns: Column[];
  defaultColumns: string[];
}

const DEFAULT_PAGE_SIZE = 10;

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

const LazyContents: React.FunctionComponent<IProps> = (props) => {
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
  const [pageSize, setPageSize] = useState<number>(DEFAULT_PAGE_SIZE);
  const [rowsState, setRowsState] = useState<RowsState>({
    page: 0,
    pageSize: pageSize,
    rows: [],
    loading: false,
  });
  type IdsToAncestors = { [key: string]: string[] };
  const [idsToAncestors, setIdsToAncestors] = useState<IdsToAncestors>({});
  useEffect(() => {
    let active = true;

    (async () => {
      setRowsState((prev) => ({ ...prev, loading: true }));
      const newItems = await props.loadItems(
        rowsState.page,
        rowsState.pageSize
      );
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
        return;
      }

      // TODO Synchronize these. (Clear rows first?)
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
          rowCount={props.rowCount}
          {...rowsState}
          paginationMode="server"
          pageSize={pageSize}
          rowsPerPageOptions={[10, 30, 100]}
          onPageChange={(page) => setRowsState((prev) => ({ ...prev, page }))}
          onPageSizeChange={(pageSize) => {
            setRowsState((prev) => ({ ...prev, pageSize }));
            setPageSize(pageSize);
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

export default LazyContents;
export type { Column, Spec };
