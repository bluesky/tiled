import * as React from "react";

import { DataGrid, GridColumnHeaderItem, GridRowParams, GridToolbar } from "@mui/x-data-grid";

import Box from "@mui/material/Box";
import Container from "@mui/material/Container";
import { components } from "../openapi_schemas";
import { useNavigate } from "react-router-dom";

const config = JSON.parse(sessionStorage.getItem("config") as string);
console.log(config.specs);

interface IProps {
  items: components["schemas"]["Resource_NodeAttributes__dict__dict_"][];
  specs: string[];
}

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

const DEFAULT_PAGE_SIZE = 10;

const Contents: React.FunctionComponent<IProps> = (props) => {
  let navigate = useNavigate();
  const gridColumns = [{ field: "id", headerName: "ID", flex: 1 }];
  gridColumns.map((column) => gridColumns.push.apply({field: column.field, headerName: column.header, flex: 1}));
  const [pageSize, setPageSize] = React.useState<number>(DEFAULT_PAGE_SIZE);
  const exportOptions = {csvOptions: {fileName: `table.csv` }, printOptions: {fileName: `table` }}  // TODO customize
  const rows = props.items.map(
    (item: components["schemas"]["Resource_NodeAttributes__dict__dict_"]) => ({
      id: item.id,
    })
  );
  type IdToAncestors = { [key: string]: string[] };
  var idsToAncestors: IdToAncestors = {};
  props.items.map(
    (item: components["schemas"]["Resource_NodeAttributes__dict__dict_"]) => {
      idsToAncestors[item.id as string] = item.attributes.ancestors;
    }
  );
  return (
    <Box sx={{ my: 4 }}>
      <Container maxWidth="lg">
        <DataGrid
          rows={rows}
          columns={gridColumns}
          pagination
          pageSize={pageSize}
          rowsPerPageOptions={[10, 30, 100]}
          onPageSizeChange={(newPageSize) => setPageSize(newPageSize)}
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
            Toolbar: GridToolbar,
          }}
          componentsProps={{ toolbar: exportOptions }}
          disableColumnFilter
          autoHeight
        />
      </Container>
    </Box>
  );
};

export default Contents;
export type { Column, Spec };
