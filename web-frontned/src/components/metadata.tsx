import { useState, useEffect } from 'react';
import Container from '@mui/material/Container';
import Typography from '@mui/material/Typography';
import Box from '@mui/material/Box';
import { metadata } from '../client';

function Metadata() {
  const [items, setItems] = useState<any[]>([]);

  useEffect(() => {
    async function loadData() {
      var result = await metadata([]);
      if (result !== undefined) {
        setItems(result);
      }
    }
    loadData();
  }, []);
  return (
    <Box sx={{ my: 4 }}>
      <Container maxWidth="sm">
      <Typography variant="h4" component="h1" gutterBottom>
        <p>found {items.entries.length} items</p>
      </Typography>
      </Container>
    </Box>
  );
}

export default Metadata;
