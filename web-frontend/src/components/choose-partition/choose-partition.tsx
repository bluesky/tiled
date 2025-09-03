import * as React from "react";

import Select, { SelectChangeEvent } from "@mui/material/Select";

import Box from "@mui/material/Box";
import FormControl from "@mui/material/FormControl";
import FormHelperText from "@mui/material/FormHelperText";
import InputLabel from "@mui/material/InputLabel";
import MenuItem from "@mui/material/MenuItem";

interface ChoosePartitionProps {
  npartitions: number;
  value: number | string;
  setValue: any;
}

const ChoosePartition: React.FunctionComponent<ChoosePartitionProps> = (
  props,
) => {
  const partitions = Array.from(Array(props.npartitions).keys());
  const handleChange = (event: SelectChangeEvent<typeof props.value>) => {
    props.setValue(event.target.value);
  };

  return (
    <Box>
      <FormControl sx={{ my: 2 }}>
        <InputLabel id="partition-select-helper-label">Partition</InputLabel>
        <Select
          labelId="partition-select-label"
          id="partition-select"
          value={props.value}
          label="Partition"
          onChange={handleChange}
        >
          {partitions.map((partition) => {
            return (
              <MenuItem key={`partition-${partition}`} value={partition}>
                {partition}
              </MenuItem>
            );
          })}
        </Select>
        <FormHelperText>A portion of the rows</FormHelperText>
      </FormControl>
    </Box>
  );
};

export default ChoosePartition;
