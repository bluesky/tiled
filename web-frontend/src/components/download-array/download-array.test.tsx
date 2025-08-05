import { render, screen, fireEvent } from "@testing-library/react";
import DownloadArray from "./download-array";
import React from "react";
import { vi, describe, it, expect, beforeEach } from "vitest";

const mockSetFormat = vi.fn();
vi.mock("../download-core/download-core", () => ({
  Download: ({ link, setFormat }: any) => (
    <div>
      <div data-testid="download-url">{link}</div>
      <button
        data-testid="select-png"
        onClick={() => {
          const format = { mimetype: "image/png" };
          setFormat(format);
          mockSetFormat(format);
        }}
      >
        Select PNG
      </button>
    </div>
  ),
}));

describe("DownloadArray", () => {
  const defaultProps = {
    name: "test-array",
    structureFamily: "array" as const,
    structure: { shape: [100, 50, 25] },
    specs: ["json"],
    link: "/api/array/test",
  };

  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe("dimension display", () => {
    it("shows array dimensions", () => {
      render(<DownloadArray {...defaultProps} />);

      expect(screen.getByText("Dimensions: 100 × 50 × 25")).toBeInTheDocument();
    });

    it("handles different dimension counts", () => {
      const props = { ...defaultProps, structure: { shape: [256, 256] } };
      render(<DownloadArray {...props} />);

      expect(screen.getByText("Dimensions: 256 × 256")).toBeInTheDocument();
    });
  });

  describe("slice input", () => {
    it("removes spaces from input", () => {
      render(<DownloadArray {...defaultProps} />);

      const input = screen.getByLabelText("Slice (Optional)");
      fireEvent.change(input, { target: { value: " :50 , :25 , ::2 " } });

      expect(input).toHaveValue(":50,:25,::2");
    });

    it("starts empty", () => {
      render(<DownloadArray {...defaultProps} />);

      const input = screen.getByLabelText("Slice (Optional)");
      expect(input).toHaveValue("");
    });
  });

  describe("examples", () => {
    it("shows examples when clicked", () => {
      render(<DownloadArray {...defaultProps} />);

      fireEvent.click(screen.getByText("Examples"));

      expect(screen.getByText(/numpy array slicing/i)).toBeInTheDocument();
      expect(screen.getByText(":50")).toBeInTheDocument();
    });
  });

  describe("format warnings", () => {
    it("warns for image formats with 3D+ arrays", () => {
      render(<DownloadArray {...defaultProps} />);

      fireEvent.click(screen.getByTestId("select-png"));

      expect(screen.getByText(/multidimensional array/i)).toBeInTheDocument();
      expect(mockSetFormat).toHaveBeenCalledWith({ mimetype: "image/png" });
    });

    it("does not warn for 2D arrays", () => {
      const props = { ...defaultProps, structure: { shape: [100, 50] } };
      render(<DownloadArray {...props} />);

      fireEvent.click(screen.getByTestId("select-png"));

      expect(
        screen.queryByText(/multidimensional array/i),
      ).not.toBeInTheDocument();
    });
  });

  describe("URL building", () => {
    it("shows empty URL initially", () => {
      render(<DownloadArray {...defaultProps} />);

      expect(screen.getByTestId("download-url")).toHaveTextContent("");
    });
  });
});
