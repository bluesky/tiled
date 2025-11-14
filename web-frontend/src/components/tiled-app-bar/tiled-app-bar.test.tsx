import { describe, it, expect, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { TiledAppBar } from "./tiled-app-bar";
import { AuthProvider } from "../../auth/auth-provider";

import * as AuthContext from "../../auth/auth-context";

describe("TiledAppBar", () => {
  const renderAppBar = (currentRoute = "/", isAuthenticated = false) => {
    vi.spyOn(AuthContext, "useAuth").mockReturnValue({
      isAuthenticated,
      user: isAuthenticated
        ? { id: "testuser", uuid: "test-uuid", type: "test-type" }
        : null,
      login: vi.fn(),
      logout: vi.fn(),
      isLoading: false,
      refreshTokens: vi.fn(),
      tokens: null,
      error: null,
      authConfig: {
        required: false,
        providers: [],
        links: {
          whoami: "",
          apikey: "",
          refresh_session: "",
          revoke_session: "",
          logout: "",
        },
      },
    });

    return render(
      <MemoryRouter initialEntries={[currentRoute]}>
        <AuthProvider>
          <TiledAppBar />
        </AuthProvider>
      </MemoryRouter>,
    );
  };

  it("shows the TILED app name", () => {
    renderAppBar();
    expect(screen.getByText("TILED")).toBeInTheDocument();
  });

  it("has a working Browse button that links to the browse page", () => {
    renderAppBar("/login", true);
    const browseButton = screen.getByRole("button", { name: "Browse" });
    expect(browseButton).toBeInTheDocument();
  });

  it("looks like a proper navigation bar", () => {
    const { container } = renderAppBar();
    const navbar = screen.getByRole("banner");
    expect(navbar).toBeInTheDocument();
    expect(navbar).toHaveClass("MuiAppBar-root");
    expect(container.querySelector(".MuiToolbar-root")).toBeInTheDocument();
  });
});
