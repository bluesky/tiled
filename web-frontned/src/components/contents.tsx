import { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import Container from '@mui/material/Container';
import Box from '@mui/material/Box';
import { search } from '../client';

import * as React from 'react';
import { DataGrid } from '@mui/x-data-grid';

const columns = [
  { field: 'id', headerName: 'ID', width: 200 },
];

interface IProps {
  segments: string[]
}
        // {items.map(id=> (
        //  <li key={"li-" + id}>
        //    <Link key={"link-" + id} to={"/node" + props.segments.map(function (segment) {return "/" + segment}) + "/" + id}>{id}</Link>
        //  </li>
        //))

const Contents: React.FunctionComponent<IProps> = (props) => {
  const [items, setItems] = useState<string[]>([]);
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
          <div style={{ height: 400, width: '100%' }}>
            <DataGrid
              rows={rows}
              columns={columns}
              pageSize={10}
            />
          </div>
        </Container>
      </Box>
    );
  } else {
    return <div>Loading...</div>
  }
}

export default Contents;
