import { useState, useEffect } from 'react';
import { Link, Route, Routes, useParams } from 'react-router-dom';
import Container from '@mui/material/Container';
import Typography from '@mui/material/Typography';
import Box from '@mui/material/Box';
import { search } from '../client';
import Node from '../routes/node'


function Contents() {
  const [results, setItems] = useState<string[]>([]);
  const params = useParams<{"id": string}>();
  const segments = (params["id"] || "").split("/")

  useEffect(() => {
    async function loadData() {
      var results = await search(segments);
      if (results !== undefined) {
        setItems(results);
      }
    }
    loadData();
  }, []);
  return (
    <Box sx={{ my: 4 }}>
      <Container maxWidth="sm">
        <ul>
        {results.map(id=> (
          <li key={"li-" + id}>
            <Link key={"link-" + id} to={id}>{id}</Link>
          </li>
        ))
        }
        </ul>
        <Routes>
          {results.map(id=> (
            <Route key={"route-" + id} path={`:id/*`} element={<Node />} />
           ))
          }
        </Routes>
      </Container>
    </Box>
  );
}

export default Contents;
