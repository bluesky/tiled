import { render, screen, fireEvent, waitFor } from "@testing-library/react";
import { vi, describe, it, expect, beforeEach, afterEach } from "vitest";
import React from "react";
import NodeLazyContents from "./node-lazy-contents";
import { SettingsContext } from "../../context/settings";

const mockNavigate = vi.fn();
vi.mock("react-router-dom", async () => {
  const actual = await vi.importActual("react-router-dom");
  return {
    ...actual,
    useNavigate: () => mockNavigate,
  };
});

vi.mock("../../client", () => ({
  search: vi.fn(),
}));

vi.mock("@mui/x-data-grid", () => ({
  DataGrid: (props: any) => (
    <div data-testid="data-grid">
      {props.loading && <div data-testid="loading">Loading...</div>}

      {/* pagination buttons for testing */}
      <button
        data-testid="next-page"
        onClick={() =>
          props.onPaginationModelChange?.({ page: 1, pageSize: 10 })
        }
      >
        Next
      </button>

      {/* Render rows for testing */}
      {props.rows?.map((row: any) => (
        <div
          key={row.id}
          data-testid={`row-${row.id}`}
          onClick={() => props.onRowClick?.({ id: row.id })}
        >
          ID: {row.id} | Name: {row.name || "No name"}
        </div>
      ))}

      <div data-testid="row-count">Total: {props.rowCount || 0}</div>
    </div>
  ),
  GridToolbarColumnsButton: () => <div>Columns</div>,
  GridToolbarContainer: ({ children }: any) => <div>{children}</div>,
  GridToolbarDensitySelector: () => <div>Density</div>,
}));

const mockSearch = vi.mocked(require("../../client").search);

const testSettings = {
  api_url: "http://localhost:5173/api/v1",
  specs: [],
  structure_families: ["container", "array", "table"],
};

const defaultProps = {
  segments: ["data"],
  columns: [
    { header: "Name", field: "name", select_metadata: "title" },
    { header: "Type", field: "type", select_metadata: "data_type" },
  ],
  defaultColumns: ["id", "name"],
  specs: ["json"],
};

const mockApiResponse = {
  data: [
    {
      id: "dataset-1",
      attributes: {
        ancestors: ["experiments"],
        metadata: {
          name: "Test Dataset",
          type: "Array",
        },
      },
    },
    {
      id: "dataset-2",
      attributes: {
        ancestors: ["experiments"],
        metadata: {
          name: "Another Dataset",
          type: "Table",
        },
      },
    },
  ],
  meta: { count: 25 },
};

describe("NodeLazyContents", () => {
  let consoleErrorSpy: any;

  beforeEach(() => {
    vi.clearAllMocks();
    mockSearch.mockResolvedValue(mockApiResponse);

    consoleErrorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
  });

  afterEach(() => {
    consoleErrorSpy?.mockRestore();
  });

  it("renders data grid component", () => {
    render(
      <SettingsContext.Provider value={testSettings}>
        <NodeLazyContents {...defaultProps} />
      </SettingsContext.Provider>,
    );

    expect(screen.getByTestId("data-grid")).toBeInTheDocument();
  });

  it("shows loading indicator while fetching data", async () => {
    mockSearch.mockImplementation(() => new Promise(() => {}));

    render(
      <SettingsContext.Provider value={testSettings}>
        <NodeLazyContents {...defaultProps} />
      </SettingsContext.Provider>,
    );

    await waitFor(() => {
      expect(screen.getByTestId("loading")).toBeInTheDocument();
    });
  });

  it("displays data after loading", async () => {
    render(
      <SettingsContext.Provider value={testSettings}>
        <NodeLazyContents {...defaultProps} />
      </SettingsContext.Provider>,
    );

    await waitFor(() => {
      expect(
        screen.getByText("ID: dataset-1 | Name: Test Dataset"),
      ).toBeInTheDocument();
      expect(
        screen.getByText("ID: dataset-2 | Name: Another Dataset"),
      ).toBeInTheDocument();
      expect(screen.getByText("Total: 25")).toBeInTheDocument();
    });
  });

  it("calls search API with correct parameters", async () => {
    render(
      <SettingsContext.Provider value={testSettings}>
        <NodeLazyContents {...defaultProps} />
      </SettingsContext.Provider>,
    );

    await waitFor(() => {
      expect(mockSearch).toHaveBeenCalledWith(
        testSettings.api_url,
        ["data"],
        expect.any(AbortSignal),
        ["metadata"],
        "{name:title,type:data_type}",
        0, // page offset
        10, // page size
      );
    });
  });

  it("handles pagination correctly", async () => {
    render(
      <SettingsContext.Provider value={testSettings}>
        <NodeLazyContents {...defaultProps} />
      </SettingsContext.Provider>,
    );

    // Wait for initial load
    await waitFor(() => {
      expect(mockSearch).toHaveBeenCalledTimes(1);
    });

    mockSearch.mockClear();

    // Trigger pagination
    fireEvent.click(screen.getByTestId("next-page"));

    await waitFor(() => {
      expect(mockSearch).toHaveBeenCalledWith(
        testSettings.api_url,
        ["data"],
        expect.any(AbortSignal),
        ["metadata"],
        "{name:title,type:data_type}",
        10, // page 1 * pageSize 10 = offset 10
        10,
      );
    });
  });

  it("navigates when clicking a row", async () => {
    render(
      <SettingsContext.Provider value={testSettings}>
        <NodeLazyContents {...defaultProps} />
      </SettingsContext.Provider>,
    );

    await waitFor(() => {
      expect(screen.getByTestId("row-dataset-1")).toBeInTheDocument();
    });

    fireEvent.click(screen.getByTestId("row-dataset-1"));

    expect(mockNavigate).toHaveBeenCalledWith("/browse/experiments/dataset-1");
  });

  it("handles empty columns configuration", async () => {
    const emptyColumnsProps = {
      ...defaultProps,
      columns: [],
    };

    render(
      <SettingsContext.Provider value={testSettings}>
        <NodeLazyContents {...emptyColumnsProps} />
      </SettingsContext.Provider>,
    );

    await waitFor(() => {
      expect(mockSearch).toHaveBeenCalledWith(
        testSettings.api_url,
        ["data"],
        expect.any(AbortSignal),
        [],
        null,
        0,
        10,
      );
    });
  });

  it("handles empty data response", async () => {
    mockSearch.mockResolvedValue({
      data: [],
      meta: { count: 0 },
    });

    render(
      <SettingsContext.Provider value={testSettings}>
        <NodeLazyContents {...defaultProps} />
      </SettingsContext.Provider>,
    );

    await waitFor(() => {
      expect(screen.getByText("Total: 0")).toBeInTheDocument();
      expect(screen.queryByTestId("row-dataset-1")).not.toBeInTheDocument();
    });
  });
});
