import * as React from 'react';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import Typography from '@mui/material/Typography';
import Modal from '@mui/material/Modal';
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
  json: unknown
}

const JSONViewer: React.FunctionComponent<IProps> = (props) => {
  const [open, setOpen] = React.useState(false);
  const handleOpen = () => setOpen(true);
  const handleClose = () => setOpen(false);

  return (
    <div>
      <Button onClick={handleOpen}>View JSON</Button>
      <Modal
        open={open}
        onClose={handleClose}
        aria-labelledby="modal-modal-title"
        aria-describedby="modal-modal-description"
      >
        <Box sx={style}>
          <Typography id="modal-modal-title" variant="h6" component="h2">
            JSON Description of Item
          </Typography>
          <SyntaxHighlighter language="json">
            {JSON.stringify(props.json, null, 2)}
          </SyntaxHighlighter>
        </Box>
      </Modal>
    </div>
  );
}

export default JSONViewer;
