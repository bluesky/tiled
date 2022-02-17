import { useState, useEffect } from 'react';
import { useParams } from 'react-router-dom';
import Container from '@mui/material/Container';
import Typography from '@mui/material/Typography';
import Box from '@mui/material/Box';
import { metadata } from '../client';
import { components } from '../openapi_schemas';
import JSONViewer from './json-viewer'

function Metadata() {
  const [item, setItem] = useState<components["schemas"]["Response_Resource_NodeAttributes__dict__dict___dict__dict_"]>();
  const params = useParams<{"*": string}>();
  const segments = (params["*"] || "").split("/").filter(function (segment) {return segment})
  console.log("metadata", segments)
  useEffect(() => {
    async function loadData() {
      var result = await metadata(segments);
      if (result !== undefined) {
        console.log(result);
        setItem(result);
      }
    }
    loadData();
  }, []);
  if (item && item.data) {
    return (
      <Box sx={{ my: 4 }}>
      <Container maxWidth="sm">
      <Typography variant="h4" component="h1" gutterBottom>
        {item.data.id || "Top"}
        <JSONViewer json={item} />
      </Typography>
      </Container>
      </Box>
    );
  }
  return <div>Loading...</div>
}

export default Metadata;
