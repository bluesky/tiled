import Download from "./download-core";
import { useState } from "react";

interface DownloadArrayProps {
  structure_family: string;
  specs: string[];
  link: string;
}

const DownloadArray: React.FunctionComponent<DownloadArrayProps> = (props) => {
  const [format, setFormat] = useState<string>("");
  const link = `${props.link}?format=${format}`;

  return (
    <Download
      format={format}
      setFormat={setFormat}
      structure_family={props.structure_family}
      link={link}
    />
  );
};

export default DownloadArray;
