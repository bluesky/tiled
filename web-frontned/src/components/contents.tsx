import { useState, useEffect } from 'react';
import { Link, useParams } from 'react-router-dom';
import Container from '@mui/material/Container';
import Box from '@mui/material/Box';
import { search } from '../client';


function Contents() {
  const [results, setItems] = useState<string[]>([]);
  const params = useParams<{"*": string}>();
  const segments = (params["*"] || "").split("/").filter(function (segment) {return segment})
  console.log("contents", segments)

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
            <Link key={"link-" + id} to={"/node" + segments.map(function (segment) {return "/" + segment}) + "/" + id}>{id}</Link>
          </li>
        ))
        }
        </ul>
      </Container>
    </Box>
  );
}

export default Contents;
