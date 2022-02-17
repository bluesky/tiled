import Container from '@mui/material/Container';
import Typography from '@mui/material/Typography';
import Box from '@mui/material/Box';
import JSONViewer from './json-viewer'

interface IProps {
  segments: string[]
  item: any
}

const NodeOverview: React.FunctionComponent<IProps> = (props) => {
  if (props.item && props.item.data) {
    return (
      <Box sx={{ my: 4 }}>
      <Container maxWidth="sm">
      <Typography variant="h4" component="h1" gutterBottom>
        {props.item.data.id || "Top"}
        <JSONViewer json={props.item} />
      </Typography>
      </Container>
      </Box>
    );
  }
  return <div>Loading...</div>
}

export { NodeOverview };
