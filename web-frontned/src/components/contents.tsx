import * as React from "react";

import { DataGrid, GridRowParams } from "@mui/x-data-grid";
import { useEffect, useState } from "react";

import Box from "@mui/material/Box";
import Container from "@mui/material/Container";
import { components } from "../openapi_schemas";
import { search } from "../client";
import { useNavigate } from "react-router-dom";

const columns = [{ field: "id", headerName: "ID", width: 200 }];

interface IProps {
  segments: string[];
}

const DEFAULT_PAGE_SIZE = 10;

const Contents: React.FunctionComponent<IProps> = (props) => {
  const [pageSize, setPageSize] = React.useState<number>(DEFAULT_PAGE_SIZE);
  const [items, setItems] = useState<
    components["schemas"]["Resource_NodeAttributes__dict__dict_"][]
  >([]);
  let navigate = useNavigate();
  // When props.segments updates, load ids of children of that path.
  useEffect(() => {
    const controller = new AbortController();
    async function loadData() {
      var items = await search(props.segments, controller.signal);
      setItems(items);
    }
    loadData();
    return () => {
      controller.abort();
    };
  }, [props.segments]);
  const rows = items.map(
    (item: components["schemas"]["Resource_NodeAttributes__dict__dict_"]) => ({
      id: item.id,
    })
  );
  type IdToAncestors = { [key: string]: string[] };
  var idsToAncestors: IdToAncestors = {};
  items.map(
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
              `/node${idsToAncestors[params.id].map(function (segment: string) {
                return "/" + segment;
              })}/${params.id}`
            );
          }}
          autoHeight
        />
      </Container>
    </Box>
  );
};

export default Contents;
