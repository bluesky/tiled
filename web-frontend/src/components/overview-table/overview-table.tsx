import * as React from "react";

import Select, { SelectChangeEvent } from "@mui/material/Select";
import { useEffect, useState } from "react";

import Alert from "@mui/material/Alert";
import Box from "@mui/material/Box";
import ChoosePartition from "../choose-partition/choose-partition";
import Container from "@mui/material/Container";
import { DataGrid } from "@mui/x-data-grid";
import FormControl from "@mui/material/FormControl";
import InputLabel from "@mui/material/InputLabel";
import MenuItem from "@mui/material/MenuItem";
import { axiosInstance } from "../../client";
import { useNavigate } from "react-router-dom";

interface IProps {
  segments: string[];
  item: any;
}

const TableOverview: React.FunctionComponent<IProps> = (props) => {
  const npartitions = props.item.data.attributes.structure.npartitions;
  const [partition, setPartition] = useState<number | string>(0);
  const [rows, setRows] = useState<any[]>([]);
  const [rowsAreLoaded, setRowsAreLoaded] = useState<boolean>(false);
  const columns = props.item.data.attributes.structure.columns;
  useEffect(() => {
    const controller = new AbortController();
    const templated_link = props.item.data.links.partition.replace(
      "{index}",
      partition,
    );
    async function loadRows() {
      const response = await axiosInstance.get(
        `${templated_link}&format=application/json-seq`,
        { signal: controller.signal, responseType: "text" },
      );
      const rows = response.data
        .split("\n")
        .filter((line: string) => line.trim() !== "")
        .map((line: string) => JSON.parse(line)) as any[];
      setRows(rows);
      setRowsAreLoaded(true);
    }
    loadRows();
    return () => {
      controller.abort();
    };
  }, [
    props.segments,
    props.item.data.links.full,
    props.item.data.links.partition,
    partition,
  ]);

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
        <Box width="100%" mt={5}>
          {npartitions > 1 ? (
            <Box>
              <Alert severity="info">
                This large table is available in <em>partitions</em> (chunks of
                rows) because the full table may be slow to download and
                display.
                <br />
                In the "Download" tab, you can request the full table as a
                single file if you wish.
              </Alert>
              <ChoosePartition
                npartitions={npartitions}
                value={partition}
                setValue={setPartitionAndClearRows}
              />
            </Box>
          ) : (
            ""
          )}
          <DataDisplay
            rows={rows}
            columns={columns}
            loading={!rowsAreLoaded}
            segments={props.segments}
          />
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
      `/browse${props.segments
        .map(function (segment) {
          return "/" + segment;
        })
        .join("")}/${column}`,
    );
  };

  return (
    <FormControl size="small" sx={{ minWidth: 160 }}>
      <InputLabel id="column-select-helper-label">Go to Column</InputLabel>
      <Select
        labelId="column-select-label"
        id="column-select"
        value=""
        label="Go to Column"
        onChange={handleChange}
      >
        {props.columns.map((column) => (
          <MenuItem key={`column-${column}`} value={column}>
            {column}
          </MenuItem>
        ))}
      </Select>
    </FormControl>
  );
};

interface IDataDisplayProps {
  columns: string[];
  rows: any[];
  loading: boolean;
  segments: string[];
}

const DEFAULT_PAGE_SIZE = 10;

/**
 * Format a cell value for display.  Floating-point numbers are rounded to 4
 * significant figures, preserving at least one decimal place so that values
 * like 10.0000001 display as "10.00" rather than "10" (which would imply an
 * integer dtype).  Non-numeric values are left as-is.
 */
function formatCellValue(value: unknown): string {
  if (typeof value === "number" && !Number.isInteger(value)) {
    // toPrecision(4) gives 4 significant figures and preserves trailing zeros
    // within that precision, e.g. 10.0000001 → "10.00", 1.23456 → "1.235",
    // 12345678.9 → "1.235e+7".  We use it as-is rather than passing through
    // parseFloat() (which would strip the trailing zeros and turn "10.00" into "10").
    return value.toPrecision(4);
  }
  return String(value ?? "");
}

const DataDisplay: React.FunctionComponent<IDataDisplayProps> = (props) => {
  const [pageSize, setPageSize] = React.useState<number>(DEFAULT_PAGE_SIZE);
  const data_columns = props.columns.map((column) => ({
    field: column,
    headerName: column,
    width: 200,
    valueFormatter: (value: unknown) => formatCellValue(value),
  }));
  const data_rows = props.rows.map((row, index) => {
    row.id = index;
    return row;
  });
  return (
    <Box>
      <DataGrid
        {...(props.loading ? { loading: true } : {})}
        rows={data_rows}
        columns={data_columns}
        pagination
        paginationModel={{ pageSize, page: 0 }}
        pageSizeOptions={[10, 30, 100]}
        onPaginationModelChange={(model) => setPageSize(model.pageSize)}
        autoHeight
      />
      {/* "Go to Column" sits in a bar flush with the DataGrid footer */}
      <Box
        sx={{
          display: "flex",
          alignItems: "center",
          px: 1,
          py: 0.5,
          borderTop: 0,
          borderColor: "divider",
          backgroundColor: "background.paper",
        }}
      >
        <VisitColumns segments={props.segments} columns={props.columns} />
      </Box>
    </Box>
  );
};

export default TableOverview;
