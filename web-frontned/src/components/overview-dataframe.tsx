import * as React from "react";

import Select, { SelectChangeEvent } from "@mui/material/Select";
import { useEffect, useState } from "react";

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
  const [rows, setRows] = useState<any[]>([]);
  const [loadedRows, setLoadedRows] = useState<boolean>(false);
  const columns = props.item.data.attributes.structure.macro.columns;
  useEffect(() => {
    const controller = new AbortController();
    async function loadRows() {
      var response = await axiosInstance.get(
        `${props.item.data.links.full}?format=application/json-seq`,
        { signal: controller.signal }
      );
      const rows = response.data
        .split("\n")
        .map((line: string) => JSON.parse(line)) as any[];
      setRows(rows);
      setLoadedRows(true);
    }
    loadRows();
    return () => {
      controller.abort();
    };
  }, [props.segments, props.item.data.links.full]);
  return (
    <Box sx={{ my: 4 }}>
      <Container maxWidth="lg">
        <VisitColumns segments={props.segments} columns={columns} />
        <Box width="100%" mt={5}>
          <Typography id="table-title" variant="h6" component="h2">
            Full Table
          </Typography>
          <DataDisplay rows={rows} columns={columns} loading={!loadedRows} />
        </Box>
      </Container>
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
      `/node${props.segments.map(function (segment) {
        return "/" + segment;
      })}/${column}`
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

const DataDisplay: React.FunctionComponent<IDataDisplayProps> = (props) => {
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
      pageSize={30}
      rowsPerPageOptions={[10, 30, 100]}
      autoHeight
    />
  );
};

export { DataFrameOverview };
