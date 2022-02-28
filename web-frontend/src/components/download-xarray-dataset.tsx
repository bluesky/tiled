import * as React from "react";

import { Download, Format } from "./download-core";

import Box from "@mui/material/Box";
import ColumnList from "./column-list";
import Stack from "@mui/material/Stack";
import { useState } from "react";

interface DownloadDatasetProps {
  name: string;
  structureFamily: string;
  macrostructure: any;
  specs: string[];
  link: string;
}

const DownloadDataset: React.FunctionComponent<DownloadDatasetProps> = (
  props
) => {
  var allColumns: string[];
  allColumns = Object.keys(props.macrostructure.data_vars);
  allColumns = allColumns.concat(Object.keys(props.macrostructure.coords));
  console.log("columns", allColumns);
  const [format, setFormat] = useState<Format>();
  const [columns, setColumns] = useState<string[]>(allColumns);
  var link: string;
  if (format !== undefined && columns.length !== 0) {
    link = `${props.link}?format=${format.mimetype}`;
    // If a subset of the columns is selected, specify them.
    // We use .join(",") here so we can use string equality.
    // You wouldn't believe me if I told you how difficult it is
    // to check Array equality in Javascript.
    if (columns.join(",") !== allColumns.join(",")) {
      const field_params = columns
        .map((column) => {
          return `&field=${column}`;
        })
        .join("");
      link = link.concat(field_params);
    }
  } else {
    link = "";
  }
  console.log(link);

  console.log(props.structureFamily, format);
  return (
    <Box>
      <Stack spacing={2} direction="column">
        <ColumnList
          heading="Variables & Coordinates"
          allColumns={allColumns}
          columns={columns}
          setColumns={setColumns}
        />
        <Download
          name={props.name}
          format={format}
          setFormat={setFormat}
          structureFamily={props.structureFamily}
          link={link}
        />
      </Stack>
    </Box>
  );
};

export default DownloadDataset;
