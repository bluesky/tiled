import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import Container from '@mui/material/Container';
import Box from '@mui/material/Box';
import { search } from '../client';

import * as React from 'react';
import { DataGrid, GridRowParams } from '@mui/x-data-grid';

const columns = [
  { field: 'id', headerName: 'ID', width: 200 },
];

interface IProps {
  segments: string[]
}

const Contents: React.FunctionComponent<IProps> = (props) => {
  const [items, setItems] = useState<string[]>([]);
  let navigate = useNavigate();
  // When props.segments updates, load ids of children of that path.
  useEffect(() => {
    async function loadData() {
      var results = await search(props.segments);
      if (results !== undefined) {
        setItems(results);
      }
    }
    loadData();
  }, [props.segments]);
  if (items !== undefined) {
    const rows = items.map((key) => ({id: key}));
    return (
      <Box sx={{ my: 4 }}>
        <Container maxWidth="lg">
          <DataGrid
            rows={rows}
            columns={columns}
            pageSize={10}
            onRowClick={(params: GridRowParams) => { navigate(`/node${props.segments.map(function (segment) {return "/" + segment})}/${params.id}`); }}
            rowsPerPageOptions={[10, 30, 100]}
            autoHeight
          />
        </Container>
      </Box>
    );
  } else {
    return <div>Loading...</div>
  }
}

export default Contents;
