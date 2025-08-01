import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
import Contents from "./contents";
import { BrowserRouter } from "react-router-dom";

const mockNavigate = vi.fn();
vi.mock("react-router-dom", async () => {
  const actual = await vi.importActual("react-router-dom");
  return {
    ...actual,
    useNavigate: () => mockNavigate,
  };
});

vi.mock("@mui/x-data-grid", () => ({
  DataGrid: (props: any) => (
    <div data-testid="data-grid">
      <select data-testid="page-size-selector">
        {props.pageSizeOptions?.map((size: number) => (
          <option key={size} value={size}>
            {size}
          </option>
        ))}
      </select>

      <div data-testid="grid-rows">
        {props.rows?.map((row: any) => (
          <div
            key={row.id}
            data-testid={`row-${row.id}`}
            onClick={() => props.onRowClick?.({ id: row.id, row })}
          >
            ID: {row.id} | Name: {row.name || "No name"} | Type:{" "}
            {row.type || "No type"}
          </div>
        ))}
      </div>

      <div data-testid="columns-config">
        {props.columns?.map((col: any) => (
          <span key={col.field} data-testid={`column-${col.field}`}>
            {col.headerName}: {col.hide ? "hidden" : "visible"}
          </span>
        ))}
      </div>

      {props.slots?.toolbar && <props.slots.toolbar />}
    </div>
  ),
  GridToolbarColumnsButton: () => (
    <button data-testid="columns-button">Columns</button>
  ),
  GridToolbarContainer: ({ children }: any) => (
    <div data-testid="toolbar">{children}</div>
  ),
  GridToolbarDensitySelector: () => (
    <button data-testid="density-button">Density</button>
  ),
}));

const createTestItem = (
  id: string,
  name: string,
  type: string,
  ancestors: string[] = [],
) =>
  ({
    id,
    attributes: {
      ancestors,
      metadata: { name, type },
    },
  }) as any;

const defaultProps = {
  items: [
    createTestItem("dataset-1", "Large Dataset", "Array", ["experiments"]),
    createTestItem("dataset-2", "Small Table", "Table", [
      "experiments",
      "subfolder",
    ]),
    createTestItem("dataset-3", "Image Data", "Array", []),
  ] as any,
  specs: [],
  columns: [
    { header: "Name", field: "name", select_metadata: "name" },
    { header: "Type", field: "type", select_metadata: "type" },
  ],
  defaultColumns: ["id", "name", "type"],
};

const renderContents = (props: any = defaultProps) => {
  return render(
    <BrowserRouter>
      <Contents {...props} />
    </BrowserRouter>,
  );
};

describe("Contents Component", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("renders data grid with toolbar", () => {
    renderContents();

    expect(screen.getByTestId("data-grid")).toBeInTheDocument();
    expect(screen.getByTestId("toolbar")).toBeInTheDocument();
    expect(screen.getByTestId("columns-button")).toBeInTheDocument();
    expect(screen.getByTestId("density-button")).toBeInTheDocument();
  });

  it("displays item data correctly", () => {
    renderContents();

    expect(
      screen.getByText("ID: dataset-1 | Name: Large Dataset | Type: Array"),
    ).toBeInTheDocument();
    expect(
      screen.getByText("ID: dataset-2 | Name: Small Table | Type: Table"),
    ).toBeInTheDocument();
    expect(
      screen.getByText("ID: dataset-3 | Name: Image Data | Type: Array"),
    ).toBeInTheDocument();
  });

  it("navigates when clicking a row", () => {
    renderContents();

    fireEvent.click(screen.getByTestId("row-dataset-1"));

    expect(mockNavigate).toHaveBeenCalledWith("/browse/experiments/dataset-1");
  });

  it("builds correct navigation path with multiple ancestors", () => {
    renderContents();

    fireEvent.click(screen.getByTestId("row-dataset-2"));

    expect(mockNavigate).toHaveBeenCalledWith(
      "/browse/experiments/subfolder/dataset-2",
    );
  });

  it("handles navigation for items with no ancestors", () => {
    renderContents();

    fireEvent.click(screen.getByTestId("row-dataset-3"));

    expect(mockNavigate).toHaveBeenCalledWith("/browse/dataset-3");
  });

  it("configures column visibility based on defaultColumns", () => {
    renderContents();

    expect(screen.getByTestId("column-id")).toHaveTextContent("ID: visible");

    expect(screen.getByTestId("column-name")).toHaveTextContent(
      "Name: visible",
    );

    expect(screen.getByTestId("column-type")).toHaveTextContent(
      "Type: visible",
    );
  });

  it("handles empty item list", () => {
    const emptyProps = {
      ...defaultProps,
      items: [] as any,
    };

    renderContents(emptyProps);

    expect(screen.getByTestId("data-grid")).toBeInTheDocument();
    expect(screen.getByTestId("grid-rows")).toBeInTheDocument();
    expect(screen.queryByTestId("row-dataset-1")).not.toBeInTheDocument();
  });
});
