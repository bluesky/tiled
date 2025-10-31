import { AuthConfig, AuthTokens, User } from "./types";

const API_BASE_URL = "";
const API_PREFIX = "/api/v1";
console.log(" Auth Service initialized with backend:", API_BASE_URL);
export const authService = {
  async getAuthConfig(): Promise<AuthConfig> {
    const response = await fetch(`${API_BASE_URL}${API_PREFIX}/`);
    
    if (!response.ok) {
      throw new Error("Failed to fetch auth configuration");
    }
    const data = await response.json();
    return data.authentication;
  },

  async loginWithPassword(
    provider: string,
    username: string,
    password: string,
  ): Promise<AuthTokens> {
    
    const url = `${API_BASE_URL}${API_PREFIX}/auth/provider/${provider}/token`;

    const formData = new URLSearchParams();
    formData.append("username", username);
    formData.append("password", password);

    const response = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
      },
      body: formData.toString(),
    });

    if (!response.ok) {
      const error = await response
        .json()
        .catch(() => ({ detail: "Login failed" }));
      throw new Error(error.detail || "Invalid credentials");
    }

    return response.json();
  },

 

  async refreshSession(refreshToken: string): Promise<AuthTokens> {
    const response = await fetch(
      `${API_BASE_URL}${API_PREFIX}/auth/session/refresh`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ refresh_token: refreshToken }),
      },
    );

    if (!response.ok) {
      throw new Error("Failed to refresh session");
    }

    return response.json();
  },

  async getCurrentUser(accessToken: string): Promise<User> {
    const response = await fetch(`${API_BASE_URL}${API_PREFIX}/auth/whoami`, {
      headers: {
        Authorization: `Bearer ${accessToken}`,
      },
    });

    if (!response.ok) {
      throw new Error("Failed to get user info");
    }

    const data = await response.json();
    return data.data; // Tiled returns user in data.data
  },

  async logout(accessToken: string): Promise<void> {
    await fetch(`${API_BASE_URL}${API_PREFIX}/auth/logout`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${accessToken}`,
      },
    });
  },

  async authenticatedFetch(
    url: string,
    accessToken: string,
    options: RequestInit = {},
  ): Promise<Response> {
    return fetch(url, {
      ...options,
      headers: {
        ...options.headers,
        Authorization: `Bearer ${accessToken}`,
      },
    });
  },
};
