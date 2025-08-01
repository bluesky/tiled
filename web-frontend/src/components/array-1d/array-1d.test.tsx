import { render, screen, fireEvent, waitFor } from "@testing-library/react";
import { vi, describe, it, expect, beforeEach } from "vitest";
import React from "react";
import { DataDisplay } from "./array-1d";

vi.mock("../../client", () => ({
  axiosInstance: {
    get: vi.fn(),
  },
}));

vi.mock("../line/line", () => ({
  ArrayLineChart: ({ data, startingIndex, name }: any) => (
    <div data-testid="mocked-chart">
      MockedChart {name} {startingIndex} {data.join(",")}
    </div>
  ),
}));

describe("DataDisplay", () => {
  const mockGet = require("../../client").axiosInstance.get;

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("shows skeleton while loading", async () => {
    mockGet.mockReturnValue(new Promise(() => {}));
    render(<DataDisplay name="test" link="/api/array" range={[0, 2]} />);
    const skeleton = document.querySelector(".MuiSkeleton-root");
    expect(skeleton).toBeInTheDocument();
  });

  it("shows chart after data loads", async () => {
    mockGet.mockResolvedValue({ data: [1, 2, 3] });
    render(<DataDisplay name="test" link="/api/array" range={[0, 3]} />);

    await waitFor(() => {
      expect(screen.getByTestId("mocked-chart")).toBeInTheDocument();
      expect(screen.getByText("MockedChart test 0 1,2,3")).toBeInTheDocument();
    });
  });

  it("switches to list view", async () => {
    mockGet.mockResolvedValue({ data: [4, 5] });
    render(<DataDisplay name="bar" link="/api/array" range={[0, 2]} />);

    // Wait for chart to load first
    await waitFor(() => {
      expect(screen.getByTestId("mocked-chart")).toBeInTheDocument();
    });

    fireEvent.click(screen.getByLabelText("List"));

    await waitFor(() => {
      expect(screen.getByText("4")).toBeInTheDocument();
      expect(screen.getByText("5")).toBeInTheDocument();
    });
  });
});
