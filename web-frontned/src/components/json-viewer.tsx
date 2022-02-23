import * as React from 'react';
import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import Skeleton from '@mui/material/Skeleton';
import SyntaxHighlighter from 'react-syntax-highlighter';

const style = {
  position: 'absolute',
  top: '50%',
  left: '50%',
  transform: 'translate(-50%, -50%)',
  width: '90%',
  height:'100%',
  display:'block',
  overflow: 'scroll',
  bgcolor: 'background.paper',
  border: '2px solid #000',
  boxShadow: 24,
  p: 4,
};

interface IProps {
  json: any
}

const JSONViewer: React.FunctionComponent<IProps> = (props) => {
  if (props.json !== undefined) {
    return (
      <Box>
        <Typography id="metadata-title" variant="h6" component="h2">
          Detailed Machine-Readable Representation of Item
        </Typography>
        <SyntaxHighlighter language="json">
          {JSON.stringify(props.json, null, 2)}
        </SyntaxHighlighter>
      </Box>
    )
  }
  return <Skeleton variant="rectangular" />

}

export default JSONViewer;
