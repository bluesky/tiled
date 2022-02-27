import * as React from "react";

import { Download, Format } from "./download-core";

import Alert from "@mui/material/Alert";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import ButtonGroup from "@mui/material/ButtonGroup";
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
  const handleToggle = (value: string) => () => {
    const currentIndex = props.columns.indexOf(value);
    const newChecked = [...props.columns];

    if (currentIndex === -1) {
      newChecked.push(value);
    } else {
      newChecked.splice(currentIndex, 1);
    }

    props.setColumns(newChecked);
  };

  return (
    <Stack spacing={1} direction="column">
      <List
        sx={{
          width: "100%",
          maxWidth: 360,
          overflow: "auto",
          maxHeight: 300,
          bgcolor: "background.paper",
        }}
        subheader={
          <ListSubheader component="div" id="nested-list-subheader">
            Columns
          </ListSubheader>
        }
      >
        {props.allColumns.map((value) => {
          const labelId = `checkbox-list-label-${value}`;

          return (
            <ListItem key={value} disablePadding>
              <ListItemButton
                role={undefined}
                onClick={handleToggle(value)}
                dense
              >
                <ListItemIcon>
                  <Checkbox
                    edge="start"
                    checked={props.columns.indexOf(value) !== -1}
                    tabIndex={-1}
                    disableRipple
                    inputProps={{ "aria-labelledby": labelId }}
                  />
                </ListItemIcon>
                <ListItemText id={labelId} primary={value} />
              </ListItemButton>
            </ListItem>
          );
        })}
      </List>
      <ButtonGroup variant="text" aria-label="check-all-or-none">
        <Button
          onClick={() => {
            props.setColumns(props.allColumns);
          }}
        >
          Select All
        </Button>
        <Button
          onClick={() => {
            props.setColumns([]);
          }}
        >
          Select None
        </Button>
      </ButtonGroup>
    </Stack>
  );
};

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
    if (columns.join(",") != props.macrostructure.columns.join(",")) {
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
