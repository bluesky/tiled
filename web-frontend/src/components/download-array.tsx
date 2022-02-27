import { Download, Format } from "./download-core";

import Alert from "@mui/material/Alert";
import Box from "@mui/material/Box";
import { useState } from "react";

interface DownloadArrayProps {
  name: string;
  structureFamily: string;
  macrostructure: any;
  specs: string[];
  link: string;
}

const DownloadArray: React.FunctionComponent<DownloadArrayProps> = (props) => {
  const [format, setFormat] = useState<Format>();
  var link: string;
  if (format !== undefined) {
    link = `${props.link}?format=${format.mimetype}`;
  } else {
    link = "";
  }

  return (
    <Box>
      <Download
        name={props.name}
        format={format}
        setFormat={setFormat}
        structureFamily={props.structureFamily}
        link={link}
      />
      {format !== undefined &&
      format.mimetype.startsWith("image/") &&
      props.macrostructure.shape.length !== 2 ? (
        <Alert sx={{ mt: 2 }} severity="warning">
          This is a multidimensional array. It may be necessary to slice a
          portion of this array to successfully export it as an image.
        </Alert>
      ) : (
        ""
      )}
    </Box>
  );
};

export default DownloadArray;
