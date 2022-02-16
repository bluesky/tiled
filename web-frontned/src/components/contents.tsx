import { useState, useEffect } from 'react';
import Container from '@mui/material/Container';
import Typography from '@mui/material/Typography';
import Box from '@mui/material/Box';
import { search } from '../client';

function Contents() {
  const [results, setItems] = useState<string[]>([]);

  useEffect(() => {
    async function loadData() {
      var results = await search([]);
      if (results !== undefined) {
        setItems(results);
      }
    }
    loadData();
  }, []);
  return (
    <Box sx={{ my: 4 }}>
      <Container maxWidth="sm">
      <Typography variant="h4" component="h1" gutterBottom>
        <p>found {results.length} results</p>
      </Typography>
      </Container>
    </Box>
  );
}

export default Contents;
