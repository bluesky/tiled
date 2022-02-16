import { useState, useEffect } from 'react';
import Container from '@mui/material/Container';
import Typography from '@mui/material/Typography';
import Box from '@mui/material/Box';
import { metadata } from '../client';
import { components } from '../openapi_schemas';

function Metadata() {
  const [item, setItem] = useState<components["schemas"]["Response_Resource_NodeAttributes__dict__dict___dict__dict_"]>();

  useEffect(() => {
    async function loadData() {
      var item = await metadata([]);
      if (item !== undefined) {
        setItem(item);
      }
    }
    loadData();
  }, []);
  if (item && item.data) {
    return (
      <Box sx={{ my: 4 }}>
      <Container maxWidth="sm">
      <Typography variant="h4" component="h1" gutterBottom>
        ID {item.data.id}
      </Typography>
      </Container>
      </Box>
    );
  }
  return <div>Loading...</div>
}

export default Metadata;
