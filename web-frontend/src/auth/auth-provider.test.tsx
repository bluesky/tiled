import { vi, describe, it, expect, beforeEach, afterEach } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import "@testing-library/jest-dom";
import axios from "axios";
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

// Mock the raw axios used for token refresh (auth-provider imports the default
// export for /auth/session/refresh). By default the refresh fails, which lets
// tests exercise the cookie-session fallback deterministically.
vi.mock("axios", () => ({
  default: { post: vi.fn(() => Promise.reject(new Error("refresh failed"))) },
}));

const get = vi.mocked(axiosInstance.get);
const rawPost = vi.mocked(axios.post);

// Build a JWT-shaped token whose payload decodes to the given claims. Only the
// payload segment needs to be valid base64url JSON for tokenManager to read it.
const makeToken = (claims: Record<string, unknown>) => {
  const payload = btoa(JSON.stringify(claims))
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");
  return `header.${payload}.sig`;
};

// A token that is still valid (expires an hour from now).
const validToken = () =>
  makeToken({ exp: Math.floor(Date.now() / 1000) + 3600 });

// A token that has already expired.
const expiredToken = () =>
  makeToken({ exp: Math.floor(Date.now() / 1000) - 3600 });

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

  it("skips the whoami probe when a valid local token is present", async () => {
    // A valid (unexpired) access token: already authenticated, no probe needed.
    tokenManager.saveTokens({
      access_token: validToken(),
      refresh_token: "refresh",
    });

    renderProvider(authRequired);

    await waitFor(() => {
      expect(screen.getByTestId("initialized")).toHaveTextContent("true");
    });
    expect(get).not.toHaveBeenCalled();
    expect(rawPost).not.toHaveBeenCalled();
    expect(screen.getByTestId("authenticated")).toHaveTextContent("true");
  });

  it("falls back to the cookie probe when refreshing an expired token fails", async () => {
    // An expired token with a refresh token that the server will reject.
    tokenManager.saveTokens({
      access_token: expiredToken(),
      refresh_token: "stale-refresh",
    });
    // The refresh attempt fails (default mock), but an API-key cookie
    // authenticates the whoami probe.
    get.mockResolvedValue({
      data: { uuid: "abc", identities: [{ id: "alice", provider: "toy" }] },
    } as any);

    renderProvider(authRequired);

    await waitFor(() => {
      expect(screen.getByTestId("initialized")).toHaveTextContent("true");
    });
    expect(rawPost).toHaveBeenCalledWith(
      "/api/v1/auth/session/refresh",
      expect.anything(),
    );
    expect(get).toHaveBeenCalledWith("/api/v1/auth/whoami");
    expect(screen.getByTestId("authenticated")).toHaveTextContent("true");
    expect(screen.getByTestId("identity")).toHaveTextContent("alice");
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
