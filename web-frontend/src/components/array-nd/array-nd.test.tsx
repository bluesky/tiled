import { render, screen, waitFor } from "@testing-library/react";
import React from "react";
import { vi, describe, expect, it, beforeEach } from "vitest";
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

// mockGet is defined before vi.mock so the factory closure can reference it.
const mockGet = vi.fn();
vi.mock("../../client", () => ({
  axiosInstance: { get: (...args: any[]) => mockGet(...args) },
}));

// Mock URL.createObjectURL/revokeObjectURL (used for PNG path)
const mockObjectUrl = "blob:http://localhost/fake";
globalThis.URL.createObjectURL = vi.fn().mockReturnValue(mockObjectUrl);
globalThis.URL.revokeObjectURL = vi.fn();

// Mock HTMLCanvasElement.getContext so canvas rendering doesn't throw in jsdom.
// we add putImageData to check if rendering happened
const putImageData = vi.fn();
HTMLCanvasElement.prototype.getContext = vi.fn(() => ({
  createImageData: (w: number, h: number) => ({
    data: new Uint8ClampedArray(w * h * 4),
  }),
  putImageData,
})) as any;

const fakeBlob = new Blob(["fake-image"], { type: "image/png" });
const fakeBuffer = new Float32Array([0, 0.5, 1]).buffer;

describe("ArrayND", () => {
  beforeEach(() => {
    mockGet.mockClear();
    putImageData.mockClear();
    // Return PNG blob for image/png requests, ArrayBuffer for octet-stream
    mockGet.mockImplementation((url: string) => {
      if (url.includes("format=image/png")) {
        return Promise.resolve({ data: fakeBlob });
      }
      return Promise.resolve({ data: fakeBuffer });
    });
  });

  // Base props: 4D grayscale array (5, 4, 3, 2) — last dim=2 is NOT a color channel
  const baseProps = {
    segments: ["test-segment"],
    link: "/api/array",
    item: {},
    structure: {
      shape: [5, 4, 3, 2],
      data_type: { kind: "f", itemsize: 4, endianness: "little" },
    },
  };

  // -------------------------------------------------------------------------
  // Grayscale (canvas) path
  // -------------------------------------------------------------------------

  it("renders grayscale canvas after fetching raw data", async () => {
    render(<ArrayND {...baseProps} />);
    await waitFor(() =>
      expect(mockGet).toHaveBeenCalledWith(
        "/api/array?format=application/octet-stream&slice=2,2",
        expect.objectContaining({ responseType: "arraybuffer" }),
      ),
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
      structure: { ...baseProps.structure, shape: [5, 4, 3000, 2] }, // stride=3
    };
    render(<ArrayND {...props} />);
    expect(
      screen.getByText(/downsampled by a factor of 3/i),
    ).toBeInTheDocument();
  });

  it("does not show planar cut text when all stack dims are size 1", () => {
    // shape (1, H, W) — stackDims=1 but size=1, no slider, no message
    const props = {
      ...baseProps,
      structure: { ...baseProps.structure, shape: [1, 300, 400] },
    };
    render(<ArrayND {...props} />);
    expect(
      screen.queryByText(/Choose a planar cut/i),
    ).not.toBeInTheDocument();
  });

  it("shows colormap selector and log-scale checkbox for grayscale", () => {
    const props = {
      ...baseProps,
      structure: { ...baseProps.structure, shape: [512, 512] },
    };
    render(<ArrayND {...props} />);
    expect(screen.getByLabelText(/colormap/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/log scale/i)).toBeInTheDocument();
  });

  it("renders 2D grayscale array with octet-stream and no slice", async () => {
    const props = {
      ...baseProps,
      structure: { ...baseProps.structure, shape: [512, 512] },
    };
    render(<ArrayND {...props} />);
    await waitFor(() =>
      expect(mockGet).toHaveBeenCalledWith(
        "/api/array?format=application/octet-stream",
        expect.objectContaining({ responseType: "arraybuffer" }),
      ),
    );
  });

  it("strides spatial dims for large grayscale arrays", async () => {
    const props = {
      ...baseProps,
      structure: { ...baseProps.structure, shape: [3000, 3000] }, // stride=3
    };
    render(<ArrayND {...props} />);
    await waitFor(() =>
      expect(mockGet).toHaveBeenCalledWith(
        "/api/array?format=application/octet-stream&slice=::3,::3",
        expect.objectContaining({ responseType: "arraybuffer" }),
      ),
    );
  });

  // -------------------------------------------------------------------------
  // Color (PNG) path
  // -------------------------------------------------------------------------

  it("renders 3D RGB color image (H, W, 3) via PNG without sliders", async () => {
    const props = {
      ...baseProps,
      structure: { ...baseProps.structure, shape: [300, 451, 3] },
    };
    render(<ArrayND {...props} />);
    const img = await waitFor(() =>
      screen.getByRole("img", { name: /Data rendered/i }),
    );
    expect(img).toHaveAttribute("src", mockObjectUrl);
    expect(mockGet).toHaveBeenCalledWith(
      "/api/array?format=image/png",
      expect.objectContaining({ responseType: "blob" }),
    );
    expect(screen.queryAllByRole("slider")).toHaveLength(0);
  });

  it("renders 4D stack of RGB images (N, H, W, 3) with one slider", async () => {
    const props = {
      ...baseProps,
      structure: { ...baseProps.structure, shape: [10, 300, 451, 3] },
    };
    render(<ArrayND {...props} />);
    await waitFor(() =>
      screen.getByRole("img", { name: /Data rendered/i }),
    );
    expect(mockGet).toHaveBeenCalledWith(
      "/api/array?format=image/png&slice=5",
      expect.objectContaining({ responseType: "blob" }),
    );
    expect(screen.queryAllByRole("slider")).toHaveLength(1);
  });

  it("includes color channel in stride slice for large RGB images", async () => {
    const props = {
      ...baseProps,
      structure: { ...baseProps.structure, shape: [3000, 3000, 3] },
    };
    render(<ArrayND {...props} />);
    await waitFor(() =>
      screen.getByRole("img", { name: /Data rendered/i }),
    );
    expect(mockGet).toHaveBeenCalledWith(
      "/api/array?format=image/png&slice=::3,::3,:",
      expect.objectContaining({ responseType: "blob" }),
    );
  });

  it("does not show colormap controls for color images", () => {
    const props = {
      ...baseProps,
      structure: { ...baseProps.structure, shape: [300, 451, 3] },
    };
    render(<ArrayND {...props} />);
    expect(screen.queryByLabelText(/colormap/i)).not.toBeInTheDocument();
    expect(screen.queryByLabelText(/log scale/i)).not.toBeInTheDocument();
  });

  // -------------------------------------------------------------------------
  // NaN handling
  // -------------------------------------------------------------------------

  // A 2-D float slice, so `n` matches W * H exactly and no sliders appear.
  const props2d = (values: number[]) => {
    mockGet.mockImplementation(() =>
      Promise.resolve({ data: new Float32Array(values).buffer }),
    );
    return {
      ...baseProps,
      structure: { ...baseProps.structure, shape: [2, 2] },
    };
  };

  it("draws a slice that contains NaN", async () => {
    render(<ArrayND {...props2d([0, NaN, 2, 3])} />);
    await waitFor(() => expect(putImageData).toHaveBeenCalled());
  });

  it("renders NaN as a transparent pixel and finite values as opaque", async () => {
    render(<ArrayND {...props2d([0, NaN, 2, 3])} />);
    await waitFor(() => expect(putImageData).toHaveBeenCalled());
    const px = putImageData.mock.calls[0][0].data as Uint8ClampedArray;
    expect(px[0 * 4 + 3]).toBe(255); // 0   -> opaque
    expect(px[1 * 4 + 3]).toBe(0); //   NaN -> transparent
    expect(px[2 * 4 + 3]).toBe(255); // 2   -> opaque
    expect(px[3 * 4 + 3]).toBe(255); // 3   -> opaque
  });

  it("draws an all-NaN slice without crashing", async () => {
    render(<ArrayND {...props2d([NaN, NaN, NaN, NaN])} />);
    await waitFor(() => expect(putImageData).toHaveBeenCalled());
    const px = putImageData.mock.calls[0][0].data as Uint8ClampedArray;
    for (let i = 0; i < 4; i++) {
      expect(px[i * 4 + 3]).toBe(0);
    }
  });

  it("clamps infinities to the ends of the colormap", async () => {
    render(<ArrayND {...props2d([-Infinity, 0, 1, Infinity])} />);
    await waitFor(() => expect(putImageData).toHaveBeenCalled());
    const px = putImageData.mock.calls.at(-1)![0].data as Uint8ClampedArray;
    // Infinities are clamped, not treated as missing data.
    for (let i = 0; i < 4; i++) {
      expect(px[i * 4 + 3]).toBe(255);
    }
    // -Infinity gets the same color as the minimum (0), +Infinity the same as
    // the maximum (1). Holds under both linear and log scaling.
    expect([px[0], px[1], px[2]]).toEqual([px[4], px[5], px[6]]);
    expect([px[12], px[13], px[14]]).toEqual([px[8], px[9], px[10]]);
  });
});
