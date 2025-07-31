import { render, screen } from "@testing-library/react";
import DownloadNode from "./download-node";
import { vi, it, expect, describe } from "vitest";
import React from "react";

describe("DownloadNode", () => {
  it("renders warning alert with correct message and severity", () => {
    render(
      <DownloadNode
        name="container1"
        structureFamily="container"
        specs={[]}
        link="/api/container1"
      />,
    );
    const alert = screen.getByRole("alert");
    expect(alert).toHaveTextContent(
      /Bulk download was only recently added to the Tiled server/,
    );
    expect(alert).toHaveClass("MuiAlert-standardWarning");
  });
});
