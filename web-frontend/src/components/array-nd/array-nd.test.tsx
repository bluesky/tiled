import { render, screen, fireEvent } from "@testing-library/react";
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

vi.mock("../components/cut-clider/cut-slider", () => ({
  default: ({ min, max, value, setValue }: any) => (
    <input
      type="range"
      min={min}
      max={max}
      value={value}
      onChange={(e) => setValue(Number(e.target.value))}
      data-testid={`cut-slider-${min}-${max}`}
    />
  ),
}));

describe("ArrayND", () => {
  const baseProps = {
    segments: ["test-segment"],
    link: "/api/array",
    item: {},
    structure: {
      shape: [5, 4, 3, 2], // 4D array
    },
  };

  it("renders image with correct URL for stride=1", () => {
    render(<ArrayND {...baseProps} />);
    const img = screen.getByRole("img", { name: /Data rendered/i });
    expect(img).toHaveAttribute("src", "/api/array?format=image/png&slice=2,2");
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

  it("handles different array shapes correctly", () => {
    const props = {
      ...baseProps,
      structure: { shape: [1, 4, 3, 2] }, // First dimension is 1
    };
    render(<ArrayND {...props} />);

    expect(screen.getByRole("img")).toBeInTheDocument();
    expect(screen.getByText(/4-dimensional array/i)).toBeInTheDocument();
  });
});
