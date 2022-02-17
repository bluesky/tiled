import { useState, useEffect } from 'react';
import Container from '@mui/material/Container';
import Typography from '@mui/material/Typography';
import Box from '@mui/material/Box';
import { metadata } from '../client';
import { components } from '../openapi_schemas';
import JSONViewer from './json-viewer'

interface IProps {
  segments: string[]
}

const NodeOverview: React.FunctionComponent<IProps> = (props) => {
  const [item, setItem] = useState<components["schemas"]["Response_Resource_NodeAttributes__dict__dict___dict__dict_"]>();
  // When props.segments updates, load metadata from that path.
  useEffect(() => {
    async function loadData() {
      var result = await metadata(props.segments);
      if (result !== undefined) {
        console.log(result);
        setItem(result);
      }
    }
    loadData();
  }, [props.segments]);
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

export { NodeOverview };
