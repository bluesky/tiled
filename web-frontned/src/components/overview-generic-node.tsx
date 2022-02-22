import Container from '@mui/material/Container';
import Typography from '@mui/material/Typography';
import LoadingButton from '@mui/lab/LoadingButton';
import Box from '@mui/material/Box';
import Stack from '@mui/material/Stack';
import JSONViewer from './json-viewer'
import Contents from '../components/contents'
import { useState, useEffect } from 'react';
import { metadata } from '../client';
import { components } from '../openapi_schemas';

interface IProps {
  segments: string[]
  item: any
}

const NodeOverview: React.FunctionComponent<IProps> = (props) => {
  const [fullItem, setFullItem] = useState<components["schemas"]["Response_Resource_NodeAttributes__dict__dict___dict__dict_"]>();
  useEffect(() => {
    async function loadData() {
      // Request all the attributes.
      var result = await metadata(props.segments, ["structure_family", "structure.macro", "structure.micro", "specs", "metadata", "sorting", "count"]);
      if (result !== undefined) {
        setFullItem(result);
      }
    }
    loadData();
  }, [props.segments]);
  if (props.item && props.item.data) {
    return (
      <Box sx={{ my: 4 }}>
        <Container maxWidth="lg">
          <Typography variant="h4" component="h1" gutterBottom>
            {props.item.data.id || "Top"}
          </Typography>
          <Stack direction="row" spacing={2}>
            { fullItem ? <JSONViewer json={fullItem} /> : <LoadingButton loading loadingIndicator="Loading...">Loading...</LoadingButton>}
          </Stack>
          <Contents segments={props.segments} />
        </Container>
      </Box>
    );
  }
  return <div>Loading...</div>
}

export { NodeOverview };
