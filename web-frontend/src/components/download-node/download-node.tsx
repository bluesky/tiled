import * as React from "react";

import Alert from "@mui/material/Alert";

interface DownloadNodeProps {
  name: string;
  structureFamily: string;
  specs: string[];
  link: string;
}

const DownloadNode: React.FunctionComponent<DownloadNodeProps> = (props) => {
  return (
    <Alert severity="warning">
      This item contains many arrays and/or tables. Bulk download was only
      recently added to the Tiled server, and it is not yet supported by this
      web interface. Go to the "View" tab and navigate to one items inside to
      download.
    </Alert>
  );
};

export default DownloadNode;
