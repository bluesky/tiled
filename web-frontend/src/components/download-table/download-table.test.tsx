import { render, screen, fireEvent } from "@testing-library/react";
import DownloadTable from "./download-table";
import React from "react";
import { vi, it, expect, describe, beforeEach } from "vitest";

vi.mock("../download-core/download-core", () => ({
  Download: ({ link, setFormat }: any) => (
    <div>
      <button
        data-testid="set-csv-format"
        onClick={() => setFormat({ mimetype: "text/csv" })}
      >
        Set CSV Format
      </button>
      <div data-testid="download-url">{link}</div>
    </div>
  ),
}));

vi.mock("../column-list/column-list", () => ({
  default: ({ heading, allColumns, columns, setColumns }: any) => (
    <div data-testid="column-list">
      <span>{heading}</span>
      <div>Selected: {columns.join(", ")}</div>
      <button data-testid="select-a-b" onClick={() => setColumns(["a", "b"])}>
        Select A & B
      </button>
      <button data-testid="select-no-columns" onClick={() => setColumns([])}>
        Clear Selection
      </button>
    </div>
  ),
}));

vi.mock("../choose-partition/choose-partition", () => ({
  default: ({ npartitions, value, setValue }: any) => (
    <div data-testid="partition-selector">
      <select
        data-testid="partition-select"
        value={value}
        onChange={(e) => setValue(Number(e.target.value))}
      >
        {Array.from({ length: npartitions }, (_, i) => (
          <option key={i} value={i}>
            Partition {i}
          </option>
        ))}
      </select>
    </div>
  ),
}));

const createTableProps = (npartitions = 1) => ({
  name: "short_table",
  structureFamily: "table",
  specs: [],
  full_link: "http://localhost:5173/api/v1/table/full/short_table",
  partition_link:
    "http://localhost:5173/api/v1/table/partition/short_table?partition={index}",
  structure: {
    npartitions,
    columns: ["A", "B", "C"],
    resizable: false,
    arrow_schema: "base64-encoded-string",
  },
});

describe("DownloadTable", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("renders column selection interface", () => {
    render(<DownloadTable {...createTableProps()} />);

    expect(screen.getByTestId("column-list")).toBeInTheDocument();
    expect(screen.getByText("Columns")).toBeInTheDocument();
  });

  it("shows partition controls for multi-partition tables", () => {
    render(<DownloadTable {...createTableProps(3)} />);

    expect(screen.getByLabelText("All rows")).toBeInTheDocument();
    expect(screen.getByTestId("partition-selector")).toBeInTheDocument();
  });

  it("hides partition controls for single-partition tables", () => {
    render(<DownloadTable {...createTableProps(1)} />);

    expect(screen.queryByLabelText("All rows")).not.toBeInTheDocument();
    expect(screen.queryByTestId("partition-selector")).not.toBeInTheDocument();
  });

  it("builds download URL with selected columns", () => {
    render(<DownloadTable {...createTableProps(3)} />);

    fireEvent.click(screen.getByTestId("select-a-b"));
    fireEvent.click(screen.getByTestId("set-csv-format"));

    const downloadUrl = screen.getByTestId("download-url").textContent;
    expect(downloadUrl).toContain("format=text/csv");
    expect(downloadUrl).toContain("field=a");
    expect(downloadUrl).toContain("field=b");
  });

  it("uses full table URL when All rows is checked", () => {
    render(<DownloadTable {...createTableProps(3)} />);

    const allRowsCheckbox = screen.getByLabelText("All rows");
    fireEvent.click(allRowsCheckbox);
    fireEvent.click(screen.getByTestId("set-csv-format"));

    const downloadUrl = screen.getByTestId("download-url").textContent;
    expect(downloadUrl).toContain("table/full/short_table");
    expect(downloadUrl).toContain("format=text/csv");
  });

  it("shows empty URL when no columns selected", () => {
    render(<DownloadTable {...createTableProps()} />);

    fireEvent.click(screen.getByTestId("select-no-columns"));
    fireEvent.click(screen.getByTestId("set-csv-format"));

    expect(screen.getByTestId("download-url").textContent).toBe("");
  });

  it("starts with all columns selected", () => {
    render(<DownloadTable {...createTableProps()} />);

    expect(screen.getByText("Selected: A, B, C")).toBeInTheDocument();
  });
});
