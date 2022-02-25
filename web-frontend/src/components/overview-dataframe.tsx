import * as React from "react";

import Select, { SelectChangeEvent } from "@mui/material/Select";
import { useEffect, useState } from "react";

import Alert from "@mui/material/Alert";
import Box from "@mui/material/Box";
import Container from "@mui/material/Container";
import { DataGrid } from "@mui/x-data-grid";
import FormControl from "@mui/material/FormControl";
import FormHelperText from "@mui/material/FormHelperText";
import InputLabel from "@mui/material/InputLabel";
import MenuItem from "@mui/material/MenuItem";
import Typography from "@mui/material/Typography";
import { axiosInstance } from "../client";
import { useNavigate } from "react-router-dom";

interface IProps {
  segments: string[];
  item: any;
}

const DataFrameOverview: React.FunctionComponent<IProps> = (props) => {
  const npartitions = props.item.data.attributes.structure.macro.npartitions;
  const [partition, setPartition] = useState<number | string>(0);
  const [rows, setRows] = useState<any[]>([]);
  const [rowsAreLoaded, setRowsAreLoaded] = useState<boolean>(false);
  const columns = props.item.data.attributes.structure.macro.columns;
  useEffect(() => {
    const controller = new AbortController();
    const templated_link = props.item.data.links.partition.replace(
      "{index}",
      partition
    );
    async function loadRows() {
      var response = await axiosInstance.get(
        `${templated_link}&format=application/json-seq`,
        { signal: controller.signal }
      );
      const rows = response.data
        .split("\n")
        .map((line: string) => JSON.parse(line)) as any[];
      setRows(rows);
      setRowsAreLoaded(true);
    }
    loadRows();
    return () => {
      controller.abort();
    };
  }, [props.segments, props.item.data.links.full, partition]);

  const setPartitionAndClearRows = (partition: number | string) => {
    // First clear the current contents and reactive the loading spinner.
    setRows([]);
    setRowsAreLoaded(false);
    // And then update the select box and begin downloading the new partition.
    setPartition(partition);
  };
  return (
    <Box sx={{ my: 4 }}>
      <Container maxWidth="lg">
        <VisitColumns segments={props.segments} columns={columns} />
        <Box width="100%" mt={5}>
          <Typography id="table-title" variant="h6" component="h2">
            Table
          </Typography>
          {npartitions > 1 ? (
            <ChoosePartition
              npartitions={npartitions}
              value={partition}
              setValue={setPartitionAndClearRows}
            />
          ) : (
            ""
          )}
          <DataDisplay rows={rows} columns={columns} loading={!rowsAreLoaded} />
        </Box>
      </Container>
    </Box>
  );
};

interface ChoosePartitionProps {
  npartitions: number;
  value: number | string;
  setValue: any;
}

const ChoosePartition: React.FunctionComponent<ChoosePartitionProps> = (
  props
) => {
  const partitions = Array.from(Array(props.npartitions).keys());
  const handleChange = (event: SelectChangeEvent<typeof props.value>) => {
    props.setValue(event.target.value);
  };

  return (
    <Box>
      <Alert severity="info">
        This large dataframe available in <em>partitions</em> (chunks of rows)
        because the full dataframe may be slow to download and display.
        <br />
        In the "Download" tab, you can request the full table as a single file
        if you wish.
      </Alert>
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

interface VisitColumnsProps {
  columns: string[];
  segments: string[];
}

const VisitColumns: React.FunctionComponent<VisitColumnsProps> = (props) => {
  let navigate = useNavigate();

  const handleChange = (event: SelectChangeEvent) => {
    const column = event.target.value;
    navigate(
      `/browse${props.segments
        .map(function (segment) {
          return "/" + segment;
        })
        .join("")}/${column}`
    );
  };

  return (
    <Box>
      <FormControl>
        <InputLabel id="column-select-helper-label">Go to Column</InputLabel>
        <Select
          labelId="column-select-label"
          id="column-select"
          value=""
          label="Column"
          onChange={handleChange}
        >
          {props.columns.map((column) => {
            return (
              <MenuItem key={`column-${column}`} value={column}>
                {column}
              </MenuItem>
            );
          })}
        </Select>
        <FormHelperText>Access a single column as an Array.</FormHelperText>
      </FormControl>
    </Box>
  );
};

interface IDataDisplayProps {
  columns: string[];
  rows: any[];
  loading: boolean;
}

const DEFAULT_PAGE_SIZE = 10;

const DataDisplay: React.FunctionComponent<IDataDisplayProps> = (props) => {
  const [pageSize, setPageSize] = React.useState<number>(DEFAULT_PAGE_SIZE);
  const data_columns = props.columns.map((column) => ({
    field: column,
    headerName: column,
    width: 200,
  }));
  const data_rows = props.rows.map((row, index) => {
    row.id = index;
    return row;
  });
  return (
    <DataGrid
      {...(props.loading ? { loading: true } : {})}
      rows={data_rows}
      columns={data_columns}
      pagination
      pageSize={pageSize}
      rowsPerPageOptions={[10, 30, 100]}
      onPageSizeChange={(newPageSize) => setPageSize(newPageSize)}
      autoHeight
    />
  );
};

export { DataFrameOverview };
