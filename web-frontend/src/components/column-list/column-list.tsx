import * as React from "react";

import Button from "@mui/material/Button";
import ButtonGroup from "@mui/material/ButtonGroup";
import Checkbox from "@mui/material/Checkbox";
import List from "@mui/material/List";
import ListItem from "@mui/material/ListItem";
import ListItemButton from "@mui/material/ListItemButton";
import ListItemIcon from "@mui/material/ListItemIcon";
import ListItemText from "@mui/material/ListItemText";
import ListSubheader from "@mui/material/ListSubheader";
import Stack from "@mui/material/Stack";

interface ColumnListProps {
  heading: string;
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
          maxWidth: 500,
          overflow: "auto",
          maxHeight: 300,
          bgcolor: "background.paper",
        }}
        subheader={
          <ListSubheader component="div" id="column-list-heading">
            {props.heading}
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

export default ColumnList;
