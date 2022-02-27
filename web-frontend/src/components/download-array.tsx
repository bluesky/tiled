import { Download, Format } from "./download-core";

import { useState } from "react";

interface DownloadArrayProps {
  name: string;
  structure_family: string;
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
    <Download
      name={props.name}
      format={format}
      setFormat={setFormat}
      structure_family={props.structure_family}
      link={link}
    />
  );
};

export default DownloadArray;
