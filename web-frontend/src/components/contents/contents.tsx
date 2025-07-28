import * as React from "react";

import {
  DataGrid,
  GridRowParams,
  GridToolbarColumnsButton,
  GridToolbarContainer,
  GridToolbarDensitySelector,
} from "@mui/x-data-grid";

import Box from "@mui/material/Box";
import Container from "@mui/material/Container";
import { components } from "../../openapi_schemas";
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
  items: components["schemas"]["Resource_NodeAttributes__dict__dict_"][];
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

const Contents: React.FunctionComponent<IProps> = (props) => {
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
    }),
  );
  const [pageSize, setPageSize] = React.useState<number>(DEFAULT_PAGE_SIZE);
  const rows = props.items.map(
    (item: components["schemas"]["Resource_NodeAttributes__dict__dict_"]) => {
      const row: { [key: string]: any } = {};
      row.id = item.id;
      props.columns.map((column) => {
        row[column.field] = item.attributes!.metadata![column.field];
        return null;
      });
      return row;
    },
  );
  type IdToAncestors = { [key: string]: string[] };
  const idsToAncestors: IdToAncestors = {};
  props.items.map(
    (item: components["schemas"]["Resource_NodeAttributes__dict__dict_"]) => {
      idsToAncestors[item.id as string] = item.attributes.ancestors;
      return null;
    },
  );
  return (
    <Box sx={{ my: 4 }}>
      <Container maxWidth="lg">
        <DataGrid
          rows={rows}
          columns={gridColumns}
          paginationModel={{ pageSize, page: 0 }}
          pageSizeOptions={[10, 30, 100]}
          onRowClick={(params: GridRowParams) => {
            navigate(
              `/browse${idsToAncestors[params.id]
                .map(function (ancestor: string) {
                  return "/" + ancestor;
                })
                .join("")}/${params.id}`,
            );
          }}
          slots={{
            toolbar: CustomToolbar,
          }}
          disableColumnFilter
          autoHeight
        />
      </Container>
    </Box>
  );
};

export default Contents;
export type { Column, Spec };
