import * as React from "react";

import { Download, Format } from "./download-core";

import Box from "@mui/material/Box";
import Checkbox from "@mui/material/Checkbox";
import ChoosePartition from "./choose-partition";
import ColumnList from "./column-list";
import FormControlLabel from "@mui/material/FormControlLabel";
import FormGroup from "@mui/material/FormGroup";
import Stack from "@mui/material/Stack";
import { useState } from "react";

interface DownloadDataFrameProps {
  name: string;
  structureFamily: string;
  macrostructure: any;
  specs: string[];
  partition_link: string;
  full_link: string;
}

const DownloadDataFrame: React.FunctionComponent<DownloadDataFrameProps> = (
  props
) => {
  const npartitions = props.macrostructure.npartitions;
  const [format, setFormat] = useState<Format>();
  const [partition, setPartition] = useState<number>(0);
  const [full, setFull] = useState<boolean>(npartitions === 1);
  const [columns, setColumns] = useState<string[]>(
    props.macrostructure.columns
  );
  var link: string;
  if (format !== undefined && columns.length !== 0) {
    if (full) {
      link = `${props.full_link}?format=${format.mimetype}`;
    } else {
      link = `${props.partition_link.replace(
        "{index}",
        partition.toString()
      )}&format=${format.mimetype}`;
    }
    // If a subset of the columns is selected, specify them.
    // We use .join(",") here so we can use string equality.
    // You wouldn't believe me if I told you how difficult it is
    // to check Array equality in Javascript.
    if (columns.join(",") !== props.macrostructure.columns.join(",")) {
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

  const handleFullCheckbox = (event: React.ChangeEvent<HTMLInputElement>) => {
    setFull(event.target.checked);
  };

  return (
    <Box>
      <Stack spacing={2} direction="column">
        <Stack spacing={1} direction="row">
          <ColumnList
            heading="Columns"
            allColumns={props.macrostructure.columns}
            columns={columns}
            setColumns={setColumns}
          />
          {npartitions > 1 ? (
            <Box>
              <FormGroup>
                <FormControlLabel
                  control={
                    <Checkbox checked={full} onChange={handleFullCheckbox} />
                  }
                  label="All rows"
                />
              </FormGroup>
              {full ? (
                ""
              ) : (
                <ChoosePartition
                  npartitions={npartitions}
                  value={partition}
                  setValue={setPartition}
                />
              )}
            </Box>
          ) : (
            ""
          )}
        </Stack>
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

export default DownloadDataFrame;
