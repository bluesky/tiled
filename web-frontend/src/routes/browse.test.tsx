import { vi, describe, it, expect, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/react";
import "@testing-library/jest-dom";
import { SettingsContext } from "../context/settings";
import { NodeTabs, OverviewDispatch, DownloadDispatch } from "./browse";
import { metadata as metadataImport } from "../client";

const metadata = vi.mocked(metadataImport);

vi.mock("../client");

vi.mock("../components/overview-array/overview-array", () => ({
  default: () => <div data-testid="array-overview">Array Overview</div>,
}));

vi.mock("../components/overview-table/overview-table", () => ({
  default: () => <div data-testid="table-overview">Table Overview</div>,
}));

vi.mock("../components/overview-generic-node/overview-generic-node", () => ({
  default: () => <div data-testid="node-overview">Node Overview</div>,
}));

vi.mock("../components/download-array/download-array", () => ({
  default: () => <div data-testid="download-array">Download Array</div>,
}));

vi.mock("../components/download-table/download-table", () => ({
  default: () => <div data-testid="download-table">Download Table</div>,
}));

vi.mock("../components/download-node/download-node", () => ({
  default: () => <div data-testid="download-node">Download Node</div>,
}));

vi.mock("../components/metadata-view/metadata-view", () => ({
  default: () => <div data-testid="metadata-view">Metadata View</div>,
}));

vi.mock("../components/json-viewer/json-viewer", () => ({
  default: () => <div data-testid="json-viewer">JSON Viewer</div>,
}));

vi.mock("../components/node-breadcrumbs/node-breadcrumbs", () => ({
  default: () => <div data-testid="breadcrumbs">Breadcrumbs</div>,
}));

const testSettings = {
  api_url: "http://localhost:5173/api/v1",
  specs: [
    {
      spec: "json",
      columns: [{ header: "ID", field: "id", select_metadata: "" }],
      default_columns: ["id"],
    },
  ],
  structure_families: ["container", "array", "table"],
};

const MockItem = (
  structureFamily: "container" | "array" | "table",
  id = "test-item",
) => {
  const baseItem = {
    data: {
      id,
      attributes: {
        ancestors: [] as string[],
        structure_family: structureFamily,
        specs: [] as string[],
        metadata: {} as { [key: string]: any },
        structure: {} as any,
        sorting: [] as { key: string; direction: 1 | -1 }[],
        access_blob: null as any,
        data_sources: null as any,
        count: undefined as number | undefined,
      },
      links: {
        self: `http://localhost:5173/api/v1/metadata/${id}`,
        full: `http://localhost:5173/api/v1/${structureFamily}/full/${id}`,
      } as any,
      meta: {} as { [key: string]: unknown },
    },
  };

  switch (structureFamily) {
    case "container":
      return {
        ...baseItem,
        data: {
          ...baseItem.data,
          attributes: {
            ...baseItem.data.attributes,
            structure: {
              contents: null,
              count: 16,
            },
            count: 16,
            sorting: [{ key: "_", direction: 1 as 1 }],
          },
          links: {
            ...baseItem.data.links,
            search: `http://localhost:5173/api/v1/search/${id}`,
          },
        },
      };

    case "array":
      return {
        ...baseItem,
        data: {
          ...baseItem.data,
          attributes: {
            ...baseItem.data.attributes,
            structure: {
              shape: [300, 300],
              data_type: {
                kind: "f",
                itemsize: 8,
              },
            },
          },
          links: {
            ...baseItem.data.links,
            block: `http://localhost:5173/api/v1/array/block/${id}?block={0},{1}`,
          },
        },
      };

    case "table":
      return {
        ...baseItem,
        data: {
          ...baseItem.data,
          attributes: {
            ...baseItem.data.attributes,
            metadata: {
              animal: "dog",
              color: "red",
            },
            structure: {
              npartitions: 1,
              columns: ["A", "B", "C"],
            },
          },
          links: {
            ...baseItem.data.links,
            partition: `http://localhost:5173/api/v1/table/partition/${id}?partition={index}`,
          },
        },
      };

    default:
      return baseItem;
  }
};

const MockContainer = (id = "test-container") => MockItem("container", id);
const MockArray = (id = "test-array") => MockItem("array", id);
const MockTable = (id = "test-table") => MockItem("table", id);

describe("NodeTabs Component", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    metadata.mockResolvedValue(MockContainer() as any);
  });

  const renderNodeTabs = (segments: string[] = ["test"]) => {
    return render(
      <SettingsContext.Provider value={testSettings}>
        <NodeTabs segments={segments} />
      </SettingsContext.Provider>,
    );
  };

  it("renders all four tabs", async () => {
    renderNodeTabs();

    await waitFor(() => {
      expect(screen.getByRole("tab", { name: "View" })).toBeInTheDocument();
      expect(screen.getByRole("tab", { name: "Download" })).toBeInTheDocument();
      expect(screen.getByRole("tab", { name: "Metadata" })).toBeInTheDocument();
      expect(screen.getByRole("tab", { name: "Detail" })).toBeInTheDocument();
    });
  });

  it("View tab selected", async () => {
    renderNodeTabs();

    await waitFor(() => {
      expect(screen.getByRole("tab", { name: "View" })).toHaveAttribute(
        "aria-selected",
        "true",
      );
    });
  });

  it("shows content for the selected tab", async () => {
    renderNodeTabs();

    await waitFor(() => {
      expect(screen.getByTestId("node-overview")).toBeInTheDocument();
    });
  });

  it("download tab selected", async () => {
    renderNodeTabs();

    await waitFor(() => {
      expect(screen.getByTestId("node-overview")).toBeInTheDocument();
    });

    fireEvent.click(screen.getByRole("tab", { name: "Download" }));

    await waitFor(() => {
      expect(screen.getByTestId("download-node")).toBeInTheDocument();
    });
  });

  it("metadata tab selected", async () => {
    renderNodeTabs();

    fireEvent.click(screen.getByRole("tab", { name: "Metadata" }));

    await waitFor(() => {
      expect(screen.getByTestId("metadata-view")).toBeInTheDocument();
    });
  });

  it("resets to first tab when segments change", async () => {
    const { rerender } = renderNodeTabs(["old"]);

    fireEvent.click(screen.getByRole("tab", { name: "Download" }));
    expect(screen.getByRole("tab", { name: "Download" })).toHaveAttribute(
      "aria-selected",
      "true",
    );

    rerender(
      <SettingsContext.Provider value={testSettings}>
        <NodeTabs segments={["new"]} />
      </SettingsContext.Provider>,
    );

    await waitFor(() => {
      expect(screen.getByRole("tab", { name: "View" })).toHaveAttribute(
        "aria-selected",
        "true",
      );
    });
  });

  it("metadata API calls", async () => {
    renderNodeTabs(["test"]);

    await waitFor(() => {
      expect(metadata).toHaveBeenCalledWith(
        expect.any(String),
        ["test"],
        expect.any(AbortSignal),
        ["structure_family", "structure", "specs"],
      );

      expect(metadata).toHaveBeenCalledWith(
        expect.any(String),
        ["test"],
        expect.any(AbortSignal),
        [
          "structure_family",
          "structure",
          "specs",
          "metadata",
          "sorting",
          "count",
        ],
      );
    });
  });
});

describe("OverviewDispatch Component", () => {
  it("shows array overview for array data", async () => {
    const arrayItem = MockArray();

    render(<OverviewDispatch segments={["test"]} item={arrayItem as any} />);

    await waitFor(() => {
      expect(screen.getByTestId("array-overview")).toBeInTheDocument();
    });
  });

  it("shows table overview for table data", async () => {
    const tableItem = MockTable();

    render(<OverviewDispatch segments={["test"]} item={tableItem as any} />);

    await waitFor(() => {
      expect(screen.getByTestId("table-overview")).toBeInTheDocument();
    });
  });

  it("shows node overview for container data", async () => {
    const containerItem = MockContainer();

    render(
      <OverviewDispatch segments={["test"]} item={containerItem as any} />,
    );

    await waitFor(() => {
      expect(screen.getByTestId("node-overview")).toBeInTheDocument();
    });
  });

  it("shows loading skeleton when no data", () => {
    const { container } = render(
      <OverviewDispatch segments={["test"]} item={undefined} />,
    );

    expect(container.querySelector(".MuiSkeleton-root")).toBeInTheDocument();
  });
});

describe("DownloadDispatch Component", () => {
  it("shows array download for array data", async () => {
    const arrayItem = MockArray();

    render(<DownloadDispatch segments={["test"]} item={arrayItem as any} />);

    await waitFor(() => {
      expect(screen.getByTestId("download-array")).toBeInTheDocument();
    });
  });

  it("shows table download for table data", async () => {
    const tableItem = MockTable();

    render(<DownloadDispatch segments={["test"]} item={tableItem as any} />);

    await waitFor(() => {
      expect(screen.getByTestId("download-table")).toBeInTheDocument();
    });
  });

  it("shows node download for container data", async () => {
    const containerItem = MockContainer();

    render(
      <DownloadDispatch segments={["test"]} item={containerItem as any} />,
    );

    await waitFor(() => {
      expect(screen.getByTestId("download-node")).toBeInTheDocument();
    });
  });
});
