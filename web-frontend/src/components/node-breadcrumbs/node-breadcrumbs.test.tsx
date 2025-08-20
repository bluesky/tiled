import { it, expect, describe } from "vitest";
import { render, screen } from "@testing-library/react";
import NodeBreadcrumbs from "./node-breadcrumbs";
import { MemoryRouter } from "react-router-dom";

const renderWithRouter = (segments: string[] | undefined) => {
  return render(
    <MemoryRouter>
      <NodeBreadcrumbs segments={segments as string[]} />
    </MemoryRouter>,
  );
};

describe("NodeBreadcrumbs", () => {
  it("builds correct cumulative navigation URLs", () => {
    renderWithRouter(["level1", "level2", "level3"]);
    expect(screen.getByRole("link", { name: "Top" })).toHaveAttribute(
      "href",
      "/browse/",
    );
    expect(screen.getByRole("link", { name: "level1" })).toHaveAttribute(
      "href",
      "/browse/level1/",
    );
    expect(screen.getByRole("link", { name: "level2" })).toHaveAttribute(
      "href",
      "/browse/level1/level2/",
    );
    expect(screen.getByRole("link", { name: "level3" })).toHaveAttribute(
      "href",
      "/browse/level1/level2/level3/",
    );
  });

  it("handles segments with special characters in URLs", () => {
    renderWithRouter(["folder with spaces", "file-with-dashes", "data.csv"]);
    expect(
      screen.getByRole("link", { name: "folder with spaces" }),
    ).toHaveAttribute("href", "/browse/folder with spaces/");
    expect(
      screen.getByRole("link", { name: "file-with-dashes" }),
    ).toHaveAttribute("href", "/browse/folder with spaces/file-with-dashes/");
  });

  it("shows fallback when segments is undefined", () => {
    renderWithRouter(undefined);
    expect(screen.getByText("...")).toBeInTheDocument();
    expect(screen.queryByRole("link")).not.toBeInTheDocument();
  });
});
