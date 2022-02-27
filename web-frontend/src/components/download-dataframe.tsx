import * as React from "react";

import { Download, Format } from "./download-core";

import Alert from "@mui/material/Alert";
import Box from "@mui/material/Box";
import Checkbox from "@mui/material/Checkbox";
import { ChoosePartition } from "./overview-dataframe";
import FormControlLabel from "@mui/material/FormControlLabel";
import FormGroup from "@mui/material/FormGroup";
import List from "@mui/material/List";
import ListItem from "@mui/material/ListItem";
import ListItemButton from "@mui/material/ListItemButton";
import ListItemIcon from "@mui/material/ListItemIcon";
import ListItemText from "@mui/material/ListItemText";
import ListSubheader from "@mui/material/ListSubheader";
import Stack from "@mui/material/Stack";
import { useState } from "react";

interface ColumnListProps {
  allColumns: string[];
  columns: any;
  setColumns: any;
}

const ColumnList: React.FunctionComponent<ColumnListProps> = (props) => {
  const [checked, setChecked] = React.useState([0]);

  const handleToggle = (value: number) => () => {
    const currentIndex = checked.indexOf(value);
    const newChecked = [...checked];

    if (currentIndex === -1) {
      newChecked.push(value);
    } else {
      newChecked.splice(currentIndex, 1);
    }

    setChecked(newChecked);
    console.log(newChecked)
  };

  return (
    <List sx={{ width: '100%', maxWidth: 360, bgcolor: 'background.paper' }}
      subheader={
        <ListSubheader component="div" id="nested-list-subheader">
          Columns
        </ListSubheader>
      }
      >
      {Array.from(Array(props.allColumns.length).keys()).map((value) => {
        const labelId = `checkbox-list-label-${value}`;

        return (
          <ListItem
            key={value}
            disablePadding
          >
            <ListItemButton role={undefined} onClick={handleToggle(value)} dense>
              <ListItemIcon>
                <Checkbox
                  edge="start"
                  checked={checked.indexOf(value) !== -1}
                  tabIndex={-1}
                  disableRipple
                  inputProps={{ 'aria-labelledby': labelId }}
                />
              </ListItemIcon>
              <ListItemText id={labelId} primary={props.allColumns[value]} />
            </ListItemButton>
          </ListItem>
        );
      })}
    </List>
  );
}

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
  if (format !== undefined) {
    if (full) {
      link = `${props.full_link}?format=${format.mimetype}`;
    } else {
      link = `${props.partition_link.replace(
        "{index}",
        partition.toString()
      )}&format=${format.mimetype}`;
    }
    // If a subset of the columns is selected, specify them.
    if (columns !== props.macrostructure.columns) {
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
      </Stack>
    </Box>
  );
};

export default DownloadDataFrame;
