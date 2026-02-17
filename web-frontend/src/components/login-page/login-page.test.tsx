import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import * as useAuthModule from "../../auth/auth-context";
import { LoginPage } from "./login-page";
import * as authApiModule from "../../auth/auth-api";

describe("LoginPage", () => {
  const mockLogin = vi.fn();
  const mockUseAuth = {
    login: mockLogin,
    logout: vi.fn(),
    refreshTokens: vi.fn(),
    user: null,
    tokens: null,
    isAuthenticated: false,
    isLoading: false,
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
  };

  beforeEach(() => {
    vi.spyOn(useAuthModule, "useAuth").mockReturnValue(mockUseAuth);
    vi.spyOn(authApiModule.authService, "getAuthConfig").mockResolvedValue({
      providers: [
        {
          provider: "pam",
          mode: "internal",
          links: {
            auth_endpoint: "",
          },
        },
      ],
      required: false,
      links: {
        whoami: "",
        apikey: "",
        refresh_session: "",
        revoke_session: "",
        logout: "",
      },
    });
    mockLogin.mockClear();
  });

  it("renders login form fields", () => {
    render(
      <MemoryRouter>
        <LoginPage />
      </MemoryRouter>,
    );
    expect(screen.getByLabelText(/username/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/password/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /login/i })).toBeInTheDocument();
  });

  it("calls login when form is submitted", async () => {
    render(
      <MemoryRouter>
        <LoginPage />
      </MemoryRouter>,
    );
    fireEvent.change(screen.getByLabelText(/username/i), {
      target: { value: "testuser" },
    });
    fireEvent.change(screen.getByLabelText(/password/i), {
      target: { value: "testpass" },
    });
    fireEvent.click(screen.getByRole("button", { name: /login/i }));
    expect(mockLogin).toHaveBeenCalled();
  });

  it("shows error message if error is present", () => {
    vi.spyOn(useAuthModule, "useAuth").mockReturnValue({
      ...mockUseAuth,
      error: "Invalid credentials",
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
    render(
      <MemoryRouter>
        <LoginPage />
      </MemoryRouter>,
    );
    expect(screen.getByText(/invalid credentials/i)).toBeInTheDocument();
  });
});
