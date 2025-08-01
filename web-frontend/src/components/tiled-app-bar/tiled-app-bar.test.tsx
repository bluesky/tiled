import { describe, it, expect } from "vitest";
import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import TiledAppBar from "./tiled-app-bar";

describe("TiledAppBar", () => {
  const renderAppBar = (currentRoute = "/") => {
    return render(
      <MemoryRouter initialEntries={[currentRoute]}>
        <TiledAppBar />
      </MemoryRouter>,
    );
  };

  it("shows the TILED app name", () => {
    renderAppBar();
    expect(screen.getByText("TILED")).toBeInTheDocument();
  });

  it("has a working Browse button that links to the browse page", () => {
    renderAppBar();
    const browseButton = screen.getByRole("link", { name: "Browse" });
    expect(browseButton).toBeInTheDocument();
    expect(browseButton).toHaveAttribute("href", "/browse/");
  });

  it("looks like a proper navigation bar", () => {
    const { container } = renderAppBar();
    const navbar = screen.getByRole("banner");
    expect(navbar).toBeInTheDocument();
    expect(navbar).toHaveClass("MuiAppBar-root");
    expect(container.querySelector(".MuiToolbar-root")).toBeInTheDocument();
  });
});
