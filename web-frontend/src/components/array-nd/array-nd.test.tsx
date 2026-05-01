import { render, screen, waitFor } from "@testing-library/react";
import React from "react";
import { vi, describe, expect, it } from "vitest";
import ArrayND from "../array-nd/array-nd";

vi.mock("@mui/material/useMediaQuery", () => ({
  default: () => false,
}));
vi.mock("@mui/material/styles", () => ({
  useTheme: () => ({
    breakpoints: {
      down: () => false,
      values: { sm: 600, md: 900, lg: 1200 },
    },
  }),
}));

// Mock axiosInstance to return a fake blob
const fakeBlob = new Blob(["fake-image"], { type: "image/png" });
const mockGet = vi.fn().mockResolvedValue({ data: fakeBlob });
vi.mock("../../client", () => ({
  axiosInstance: { get: (...args: any[]) => mockGet(...args) },
}));

// Mock URL.createObjectURL/revokeObjectURL
const mockObjectUrl = "blob:http://localhost/fake";
globalThis.URL.createObjectURL = vi.fn().mockReturnValue(mockObjectUrl);
globalThis.URL.revokeObjectURL = vi.fn();

describe("ArrayND", () => {
  const baseProps = {
    segments: ["test-segment"],
    link: "/api/array",
    item: {},
    structure: {
      shape: [5, 4, 3, 2], // 4D array
    },
  };

  it("renders image after fetching via axiosInstance", async () => {
    render(<ArrayND {...baseProps} />);
    const img = await waitFor(() =>
      screen.getByRole("img", { name: /Data rendered/i }),
    );
    expect(img).toHaveAttribute("src", mockObjectUrl);
    expect(mockGet).toHaveBeenCalledWith(
      "/api/array?format=image/png&slice=2,2",
      expect.objectContaining({ responseType: "blob" }),
    );
  });

  it("shows sliders for multi-dimensional arrays", () => {
    render(<ArrayND {...baseProps} />);

    const sliders = screen.getAllByRole("slider");
    expect(sliders.length).toBeGreaterThan(0);
  });

  it("shows planar cut info text", () => {
    render(<ArrayND {...baseProps} />);
    expect(
      screen.getByText(/Choose a planar cut through this 4-dimensional array/i),
    ).toBeInTheDocument();
  });

  it("shows downsampling alert for large arrays", () => {
    const props = {
      ...baseProps,
      structure: { shape: [5, 4, 3000, 2] }, // stride=3
    };
    render(<ArrayND {...props} />);
    expect(
      screen.getByText(/downsampled by a factor of 3/i),
    ).toBeInTheDocument();
  });

  it("handles different array shapes correctly", async () => {
    const props = {
      ...baseProps,
      structure: { shape: [1, 4, 3, 2] }, // First dimension is 1
    };
    render(<ArrayND {...props} />);

    await waitFor(() => expect(screen.getByRole("img")).toBeInTheDocument());
    expect(screen.getByText(/4-dimensional array/i)).toBeInTheDocument();
  });

  it("renders 2D array without slice parameter", async () => {
    const props = {
      ...baseProps,
      structure: { shape: [512, 512] },
    };
    render(<ArrayND {...props} />);
    await waitFor(() =>
      screen.getByRole("img", { name: /Data rendered/i }),
    );
    expect(mockGet).toHaveBeenCalledWith(
      "/api/array?format=image/png",
      expect.objectContaining({ responseType: "blob" }),
    );
  });

  it("renders 2D array with stride using slice parameter", async () => {
    const props = {
      ...baseProps,
      structure: { shape: [3000, 3000] }, // stride=3
    };
    render(<ArrayND {...props} />);
    await waitFor(() =>
      screen.getByRole("img", { name: /Data rendered/i }),
    );
    expect(mockGet).toHaveBeenCalledWith(
      "/api/array?format=image/png&slice=::3,::3",
      expect.objectContaining({ responseType: "blob" }),
    );
  });
});
