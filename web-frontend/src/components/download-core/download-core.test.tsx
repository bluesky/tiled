import { render, screen, fireEvent } from "@testing-library/react";
import React from "react";
import { Download } from "./download-core";
import { vi, expect, describe, it, beforeEach } from "vitest";
import { SettingsContext } from "../../context/settings";

vi.mock("clipboard-copy", () => ({
  default: vi.fn(),
}));

vi.mock("../../client", () => ({
  about: vi.fn(),
  axiosInstance: {
    get: vi.fn().mockResolvedValue({ data: new Blob(["data"]) }),
  },
}));

const testFormats = [
  { mimetype: "image/png", display_name: "PNG Image", extension: ".png" },
  { mimetype: "text/csv", display_name: "CSV File", extension: ".csv" },
  {
    mimetype: "application/json",
    display_name: "JSON Data",
    extension: ".json",
  },
];

const mockSettings = {
  api_url: "http://localhost:8000/api/v1",
  specs: [],
  structure_families: {
    array: { formats: testFormats },
    table: { formats: testFormats },
    container: { formats: testFormats },
  },
};

const renderWithSettings = (props: any) => {
  return render(
    <SettingsContext.Provider value={mockSettings}>
      <Download {...props} />
    </SettingsContext.Provider>,
  );
};

describe("Download Component", () => {
  const baseProps = {
    name: "my-data",
    structureFamily: "array",
    format: testFormats[0], // PNG selected
    setFormat: vi.fn(),
    link: "/api/v1/array/full/my-data?format=image/png",
  };

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("shows available download formats", () => {
    renderWithSettings(baseProps);

    const formatSelect = screen.getByRole("combobox");
    fireEvent.mouseDown(formatSelect);

    expect(
      screen.getByRole("option", { name: "PNG Image" }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("option", { name: "CSV File" }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("option", { name: "JSON Data" }),
    ).toBeInTheDocument();
  });

  it("lets users change the download format", () => {
    renderWithSettings(baseProps);

    const formatSelect = screen.getByRole("combobox");
    fireEvent.mouseDown(formatSelect);

    const csvOption = screen.getByRole("option", { name: "CSV File" });
    fireEvent.click(csvOption);

    expect(baseProps.setFormat).toHaveBeenCalledWith(testFormats[1]);
  });

  it("renders download button that triggers blob download", () => {
    renderWithSettings(baseProps);

    const downloadButton = screen.getByRole("button", { name: /Download/i });
    expect(downloadButton).toBeInTheDocument();
    expect(downloadButton).not.toBeDisabled();
  });

  it("renders open button that triggers blob open", () => {
    renderWithSettings(baseProps);

    const openButton = screen.getByRole("button", { name: /Open/i });
    expect(openButton).toBeInTheDocument();
    expect(openButton).not.toBeDisabled();
  });

  it("shows shareable link in popover", () => {
    renderWithSettings(baseProps);

    fireEvent.click(screen.getByRole("button", { name: /Link/i }));

    const linkField = screen.getByLabelText("Link");
    expect(linkField).toHaveValue(
      "/api/v1/array/full/my-data?format=image/png",
    );
  });

  it("disables actions when no link is available", () => {
    renderWithSettings({ ...baseProps, link: "" });

    const downloadButton = screen.getByRole("button", { name: /Download/i });
    const openButton = screen.getByRole("button", { name: /Open/i });

    expect(downloadButton).toBeDisabled();
    expect(openButton).toBeDisabled();
  });
});
