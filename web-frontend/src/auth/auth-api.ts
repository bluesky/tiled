import { AuthConfig, AuthTokens, User } from "./types";
const API_BASE = "/api/v1";

export const authService = {
  async getAuthConfig(): Promise<AuthConfig> {
    const res = await fetch(`${API_BASE}/`);
    if (!res.ok) throw new Error("Failed to fetch auth config");

    const data = await res.json();
    return data.authentication;
  },

  async loginWithPassword(
    provider: string,
    username: string,
    password: string,
  ): Promise<AuthTokens> {
    const body = new URLSearchParams({ username, password });

    const res = await fetch(`${API_BASE}/auth/provider/${provider}/token`, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body,
    });

    if (!res.ok) {
      const err = await res.json().catch(() => ({ detail: "Login failed" }));
      throw new Error(err.detail || "Invalid credentials");
    }

    return res.json();
  },

  async refreshSession(refreshToken: string): Promise<AuthTokens> {
    const res = await fetch(`${API_BASE}/auth/session/refresh`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ refresh_token: refreshToken }),
    });

    if (!res.ok) throw new Error("Failed to refresh session");
    return res.json();
  },

  async getCurrentUser(accessToken: string): Promise<User> {
    const res = await fetch(`${API_BASE}/auth/whoami`, {
      headers: { Authorization: `Bearer ${accessToken}` },
    });

    if (!res.ok) throw new Error("Failed to get user info");

    const data = await res.json();
    return data.data;
  },

  async logout(accessToken: string): Promise<void> {
    await fetch(`${API_BASE}/auth/logout`, {
      method: "POST",
      headers: { Authorization: `Bearer ${accessToken}` },
    });
  },
};
