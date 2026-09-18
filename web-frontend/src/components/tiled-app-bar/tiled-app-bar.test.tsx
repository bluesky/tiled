import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import TiledAppBar from "./tiled-app-bar";
import { AuthContext, AuthContextType } from "../../auth/auth-context";
import { axiosInstance } from "../../client";
import { tokenManager } from "../../auth/token-manager";

vi.mock("../../client", () => ({
  axiosInstance: { post: vi.fn() },
}));

const post = vi.mocked(axiosInstance.post);

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

  it("has a clickable TILED logo that links to the browse page", () => {
    renderAppBar();
    const homeLink = screen.getByRole("link", { name: /TILED/i });
    expect(homeLink).toBeInTheDocument();
    expect(homeLink).toHaveAttribute("href", "/browse/");
  });

  it("looks like a proper navigation bar", () => {
    const { container } = renderAppBar();
    const navbar = screen.getByRole("banner");
    expect(navbar).toBeInTheDocument();
    expect(navbar).toHaveClass("MuiAppBar-root");
    expect(container.querySelector(".MuiToolbar-root")).toBeInTheDocument();
  });
});

describe("TiledAppBar cookie-authenticated logout", () => {
  const onLogout = vi.fn();

  const authValue: AuthContextType = {
    authRequired: true,
    providers: [],
    isAuthenticated: true,
    initialized: true,
    identity: { id: "alice", provider: "toy" },
    onLogin: vi.fn(),
    onLogout,
  };

  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.clear();
    post.mockResolvedValue({} as any);
  });

  const renderAuthed = () =>
    render(
      <MemoryRouter initialEntries={["/browse/"]}>
        <AuthContext.Provider value={authValue}>
          <TiledAppBar />
        </AuthContext.Provider>
      </MemoryRouter>,
    );

  it("shows the identity and a logout menu when authenticated", () => {
    renderAuthed();
    expect(
      screen.getByRole("button", { name: "alice" }),
    ).toBeInTheDocument();
  });

  it("clears the API-key cookie via /auth/logout when there is no refresh token", async () => {
    renderAuthed();

    fireEvent.click(screen.getByRole("button", { name: "alice" }));
    fireEvent.click(screen.getByText("Log out"));

    await waitFor(() => {
      expect(post).toHaveBeenCalledWith("/api/v1/auth/logout");
    });
    // No refresh token present, so session/revoke is not called.
    expect(post).not.toHaveBeenCalledWith(
      "/api/v1/auth/session/revoke",
      expect.anything(),
    );
    expect(onLogout).toHaveBeenCalled();
  });

  it("revokes the session and clears the cookie when a refresh token exists", async () => {
    tokenManager.saveTokens({
      access_token: "header.payload.sig",
      refresh_token: "refresh-token",
    });

    renderAuthed();

    fireEvent.click(screen.getByRole("button", { name: "alice" }));
    fireEvent.click(screen.getByText("Log out"));

    await waitFor(() => {
      expect(post).toHaveBeenCalledWith("/api/v1/auth/logout");
    });
    expect(post).toHaveBeenCalledWith("/api/v1/auth/session/revoke", {
      refresh_token: "refresh-token",
    });
    expect(onLogout).toHaveBeenCalled();
  });
});
