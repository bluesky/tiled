import * as React from "react";
import Box from "@mui/material/Box";
import SyntaxHighlighter from "react-syntax-highlighter";
import Skeleton from "@mui/material/Skeleton";
import yaml from "js-yaml";

interface IProps {
  json: any;
}

const MetadataView: React.FunctionComponent<IProps> = (props) => {
  if (props.json !== undefined) {
    const metadata = props!.json!.data!.attributes!.metadata;
    if (Object.keys(metadata).length === 0) {
      return <div>This item's metadata is empty.</div>;
    }
    return (
      <Box>
        <SyntaxHighlighter language="yaml">
          {yaml.dump(metadata)}
        </SyntaxHighlighter>
      </Box>
    );
  }
  return <Skeleton variant="rectangular" />;
};

export default MetadataView;
