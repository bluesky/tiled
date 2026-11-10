import * as React from "react";

import Box from "@mui/material/Box";
import Skeleton from "@mui/material/Skeleton";
import SyntaxHighlighter from "react-syntax-highlighter";

interface IProps {
  json: any;
}

const JSONViewer: React.FunctionComponent<IProps> = (props) => {
  if (props.json !== undefined) {
    return (
      <Box>
        <SyntaxHighlighter language="json">
          {JSON.stringify(props.json, null, 2)}
        </SyntaxHighlighter>
      </Box>
    );
  }
  return <Skeleton variant="rectangular" />;
};

export default JSONViewer;
