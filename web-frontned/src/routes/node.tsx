import { Link, Outlet } from "react-router-dom";
import React, { useState, useEffect } from 'react';
import Container from '@mui/material/Container';
import Typography from '@mui/material/Typography';
import Box from '@mui/material/Box';

import { IDs, search } from '../client';

function NodePage() {
  const [items, setItems] = useState<IDs>({"entries": []});
  useEffect(() => {
    async function loadData() {
      var result = await search([]);
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
      <Outlet />
    </Box>
  );
}

export default NodePage;
