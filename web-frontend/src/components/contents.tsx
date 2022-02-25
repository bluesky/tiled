import * as React from "react";

import { DataGrid, GridRowParams } from "@mui/x-data-grid";

import Box from "@mui/material/Box";
import Container from "@mui/material/Container";
import { components } from "../openapi_schemas";
import { useNavigate } from "react-router-dom";

const columns = [{ field: "id", headerName: "ID", width: 200 }];

interface IProps {
  items: components["schemas"]["Resource_NodeAttributes__dict__dict_"][];
}

const DEFAULT_PAGE_SIZE = 10;

const Contents: React.FunctionComponent<IProps> = (props) => {
  let navigate = useNavigate();
  const [pageSize, setPageSize] = React.useState<number>(DEFAULT_PAGE_SIZE);
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
          columns={columns}
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
          autoHeight
        />
      </Container>
    </Box>
  );
};

export default Contents;
