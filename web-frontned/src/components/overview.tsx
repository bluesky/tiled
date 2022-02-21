import Container from '@mui/material/Container';
import Typography from '@mui/material/Typography';
import Box from '@mui/material/Box';
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
        { fullItem ? <JSONViewer json={fullItem} /> : <div>Loading...</div>}
        <Contents segments={props.segments} />
      </Typography>
      </Container>
      </Box>
    );
  }
  return <div>Loading...</div>
}


const ArrayOverview: React.FunctionComponent<IProps> = (props) => {
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
        { fullItem ? <JSONViewer json={fullItem} /> : <div>Loading...</div>}
        { fullItem ? <img alt="Data rendered" src={fullItem.data!.links!.full as string} loading="lazy" /> : <div>Loading...</div> }
      </Typography>
      </Container>
      </Box>
    );
  }
  return <div>Loading...</div>
}

export { NodeOverview, ArrayOverview };
