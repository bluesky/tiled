import { render, screen, fireEvent } from "@testing-library/react";
import RangeSlider from "./range-slider";
import { vi, it, expect, describe } from "vitest";
import React, { useState } from "react";

function TestWrapper({
  min = 0,
  max = 100,
  initialValue = [10, 20],
  limit = 50,
}) {
  const [value, setValue] = useState<number[]>(initialValue);

  return (
    <div>
      <RangeSlider
        min={min}
        max={max}
        value={value}
        setValue={setValue}
        limit={limit}
      />

      <span data-testid="min-value">{value[0]}</span>
      <span data-testid="max-value">{value[1]}</span>
    </div>
  );
}

describe("RangeSlider", () => {
  it("displays current range values", () => {
    render(<TestWrapper initialValue={[15, 35]} />);

    expect(screen.getByTestId("min-value")).toHaveTextContent("15");
    expect(screen.getByTestId("max-value")).toHaveTextContent("35");
  });

  describe("limit enforcement", () => {
    it("adjusts max when min input exceeds limit", () => {
      render(<TestWrapper limit={10} initialValue={[5, 15]} />);

      const minInput = screen.getAllByRole("spinbutton")[0];
      fireEvent.change(minInput, { target: { value: "20" } });

      expect(screen.getByTestId("min-value")).toHaveTextContent("20");
      expect(screen.getByTestId("max-value")).toHaveTextContent("15");
    });

    it("adjusts min when max input exceeds limit", () => {
      render(<TestWrapper limit={15} initialValue={[10, 25]} />);

      const maxInput = screen.getAllByRole("spinbutton")[1];
      fireEvent.change(maxInput, { target: { value: "50" } });

      expect(screen.getByTestId("max-value")).toHaveTextContent("50");
      expect(screen.getByTestId("min-value")).toHaveTextContent("35");
    });
  });

  describe("empty input handling", () => {
    it("ignores empty input values", () => {
      render(<TestWrapper initialValue={[20, 40]} />);

      const minInput = screen.getAllByRole("spinbutton")[0];
      fireEvent.change(minInput, { target: { value: "" } });

      expect(screen.getByTestId("min-value")).toHaveTextContent("20");
      expect(screen.getByTestId("max-value")).toHaveTextContent("40");
    });
  });
});
