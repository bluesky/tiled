import * as React from "react";

import Select, { SelectChangeEvent } from "@mui/material/Select";
import { useEffect, useState } from "react";

import Alert from "@mui/material/Alert";
import Box from "@mui/material/Box";
import Checkbox from "@mui/material/Checkbox";
import ChoosePartition from "../choose-partition/choose-partition";
import Container from "@mui/material/Container";
import { DataGrid } from "@mui/x-data-grid";
import FormControl from "@mui/material/FormControl";
import FormControlLabel from "@mui/material/FormControlLabel";
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
    let active = true;
    const controller = new AbortController();
    const templated_link = props.item.data.links.partition.replace(
      "{index}",
      partition,
    );

    (async () => {
      try {
        const response = await axiosInstance.get(
          `${templated_link}&format=application/json-seq`,
          { signal: controller.signal, responseType: "text" },
        );
        const parsed = response.data
          .split("\n")
          .filter((line: string) => line.trim() !== "")
          .map((line: string) => JSON.parse(line)) as any[];
        if (!active) return;
        setRows(parsed);
        setRowsAreLoaded(true);
      } catch (err: any) {
        if (err?.name === "CanceledError" || err?.name === "AbortError") return;
        if (!active) return;
        // On error, mark as loaded (stops the spinner) with empty rows.
        setRowsAreLoaded(true);
      }
    })();

    return () => {
      active = false;
      controller.abort();
    };
  }, [
    props.segments,
    props.item.data.links.full,
    props.item.data.links.partition,
    partition,
  ]);

  const setPartitionAndClearRows = (partition: number | string) => {
    setRows([]);
    setRowsAreLoaded(false);
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
  const navigate = useNavigate();

  const handleChange = (event: SelectChangeEvent) => {
    const column = event.target.value;
    navigate(
      `/browse${props.segments.map((segment) => "/" + segment).join("")}/${column}`,
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
 * Format a cell value for display. Floating-point numbers are rounded to 4
 * significant figures, preserving trailing zeros (e.g. 10.0000001 → "10.00",
 * 1.23456 → "1.235"). Non-numeric values are left as-is.
 */
function formatCellValue(value: unknown): string {
  if (typeof value === "number" && !Number.isInteger(value)) {
    return value.toPrecision(4);
  }
  return String(value ?? "");
}

const DataDisplay: React.FunctionComponent<IDataDisplayProps> = (props) => {
  const [pageSize, setPageSize] = React.useState<number>(DEFAULT_PAGE_SIZE);
  const [page, setPage] = React.useState<number>(0);
  const [transposed, setTransposed] = React.useState<boolean>(false);
  const [autoApplied, setAutoApplied] = React.useState<boolean>(false);

  // Reset pagination, transpose, and auto-apply flag when the table identity
  // changes (different segments = different table node).
  React.useEffect(() => {
    setPage(0);
    setTransposed(false);
    setAutoApplied(false);
  }, [props.segments, props.columns]);

  // Auto-transpose once when data first arrives: if columns outnumber rows,
  // a transposed layout is easier to read.
  React.useEffect(() => {
    if (!props.loading && props.rows.length > 0 && !autoApplied) {
      setAutoApplied(true);
      if (props.columns.length > props.rows.length) {
        setTransposed(true);
      }
    }
  }, [props.loading, props.rows.length, props.columns.length, autoApplied]);

  let data_columns;
  let data_rows;

  if (!transposed) {
    data_columns = props.columns.map((column) => ({
      field: column,
      headerName: column,
      width: 200,
      valueFormatter: (value: unknown) => formatCellValue(value),
    }));
    data_rows = props.rows.map((row, index) => ({ ...row, id: index }));
  } else {
    // Transposed: original column names become row labels; original rows
    // become columns named row_0, row_1, …
    data_columns = [
      { field: "__column__", headerName: "Column", width: 160 },
      ...props.rows.map((_, i) => ({
        field: `row_${i}`,
        headerName: `row_${i}`,
        width: 160,
        valueFormatter: (value: unknown) => formatCellValue(value),
      })),
    ];
    data_rows = props.columns.map((col) => {
      const row: Record<string, any> = { id: col, __column__: col };
      props.rows.forEach((r, i) => {
        row[`row_${i}`] = r[col];
      });
      return row;
    });
  }

  return (
    <Box>
      <DataGrid
        {...(props.loading ? { loading: true } : {})}
        rows={data_rows}
        columns={data_columns}
        pagination
        paginationModel={{ pageSize, page }}
        pageSizeOptions={[10, 30, 100]}
        onPaginationModelChange={(model) => {
          setPage(model.page);
          setPageSize(model.pageSize);
        }}
        autoHeight
      />
      {/* Footer bar: "Go to Column" and "Transpose" at the same level as "Rows per page" */}
      <Box
        sx={{
          display: "flex",
          alignItems: "center",
          gap: 2,
          px: 1,
          py: 0.5,
          backgroundColor: "background.paper",
        }}
      >
        {!transposed && (
          <VisitColumns segments={props.segments} columns={props.columns} />
        )}
        <FormControlLabel
          control={
            <Checkbox
              checked={transposed}
              onChange={(e) => setTransposed(e.target.checked)}
              size="small"
            />
          }
          label="Transpose"
          sx={{ ml: "auto" }}
        />
      </Box>
    </Box>
  );
};

export default TableOverview;
