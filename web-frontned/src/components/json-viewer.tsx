import * as React from "react";

import Box from "@mui/material/Box";
import Skeleton from "@mui/material/Skeleton";
import SyntaxHighlighter from "react-syntax-highlighter";
import Typography from "@mui/material/Typography";

interface IProps {
  json: any;
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
    );
  }
  return <Skeleton variant="rectangular" />;
};

export default JSONViewer;
