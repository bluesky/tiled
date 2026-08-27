import { vi, describe, it, expect, beforeEach, afterEach } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import "@testing-library/jest-dom";
import { AuthProvider } from "./auth-provider";
import { useAuth } from "./auth-context";
import { axiosInstance } from "../client";
import { tokenManager } from "./token-manager";

vi.mock("../client", () => {
  const interceptor = { use: vi.fn(() => 0), eject: vi.fn() };
  return {
    axiosInstance: {
      get: vi.fn(),
      post: vi.fn(),
      interceptors: { request: interceptor, response: interceptor },
    },
  };
});

const get = vi.mocked(axiosInstance.get);

// Minimal server auth config that requires authentication.
const authRequired = {
  required: true,
  providers: [],
} as any;

function Probe() {
  const { isAuthenticated, initialized, identity } = useAuth();
  return (
    <div>
      <span data-testid="initialized">{String(initialized)}</span>
      <span data-testid="authenticated">{String(isAuthenticated)}</span>
      <span data-testid="identity">{identity ? identity.id : "none"}</span>
    </div>
  );
}

const renderProvider = (authentication: any) =>
  render(
    <AuthProvider authentication={authentication}>
      <Probe />
    </AuthProvider>,
  );

describe("AuthProvider cookie detection", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.clear();
  });

  afterEach(() => {
    localStorage.clear();
  });

  it("treats a cookie-authenticated session as authenticated via whoami", async () => {
    get.mockResolvedValue({
      data: { uuid: "abc", identities: [{ id: "alice", provider: "toy" }] },
    } as any);

    renderProvider(authRequired);

    await waitFor(() => {
      expect(screen.getByTestId("initialized")).toHaveTextContent("true");
    });
    expect(get).toHaveBeenCalledWith("/api/v1/auth/whoami");
    expect(screen.getByTestId("authenticated")).toHaveTextContent("true");
    expect(screen.getByTestId("identity")).toHaveTextContent("alice");
  });

  it("remains unauthenticated when whoami returns null", async () => {
    get.mockResolvedValue({ data: null } as any);

    renderProvider(authRequired);

    await waitFor(() => {
      expect(screen.getByTestId("initialized")).toHaveTextContent("true");
    });
    expect(screen.getByTestId("authenticated")).toHaveTextContent("false");
  });

  it("does not become initialized until the whoami probe resolves", async () => {
    let resolve: (v: any) => void = () => {};
    get.mockReturnValue(
      new Promise((r) => {
        resolve = r;
      }) as any,
    );

    renderProvider(authRequired);

    // Probe in flight: must not be initialized yet (prevents flash redirect).
    expect(screen.getByTestId("initialized")).toHaveTextContent("false");

    resolve({ data: null });
    await waitFor(() => {
      expect(screen.getByTestId("initialized")).toHaveTextContent("true");
    });
  });

  it("skips the whoami probe when local tokens are present", async () => {
    // A non-expired-looking access token so hasTokens() is true.
    tokenManager.saveTokens({
      access_token: "header.payload.sig",
      refresh_token: "refresh",
    });

    renderProvider(authRequired);

    await waitFor(() => {
      expect(screen.getByTestId("initialized")).toHaveTextContent("true");
    });
    expect(get).not.toHaveBeenCalled();
    expect(screen.getByTestId("authenticated")).toHaveTextContent("true");
  });

  it("skips the whoami probe when the server does not require auth", async () => {
    renderProvider({ required: false, providers: [] } as any);

    await waitFor(() => {
      expect(screen.getByTestId("initialized")).toHaveTextContent("true");
    });
    expect(get).not.toHaveBeenCalled();
    expect(screen.getByTestId("authenticated")).toHaveTextContent("false");
  });
});
