import { AuthTokens, UserIdentity } from "./types";

const ACCESS_TOKEN_KEY = "tiled_access_token";
const REFRESH_TOKEN_KEY = "tiled_refresh_token";
const IDENTITY_KEY = "tiled_identity";
const TOKEN_EXPIRY_BUFFER_MS = 60 * 1000; // 60 seconds

// Expose token globally so external spec_view scripts can use it.
declare global {
  interface Window {
    __TILED_ACCESS_TOKEN__?: string | null;
  }
}

interface JWTPayload {
  exp?: number;
  [key: string]: unknown;
}

function decodeJWT(token: string): JWTPayload | null {
  try {
    const [, payload] = token.split(".");
    if (!payload) return null;
    const base64 = payload.replace(/-/g, "+").replace(/_/g, "/");
    const padded = base64 + "=".repeat((4 - (base64.length % 4)) % 4);
    return JSON.parse(atob(padded));
  } catch {
    return null;
  }
}

class TokenManager {
  constructor() {
    try {
      window.__TILED_ACCESS_TOKEN__ = localStorage.getItem(ACCESS_TOKEN_KEY);
    } catch {
      // localStorage not available (e.g., in tests)
    }
  }

  saveTokens(tokens: AuthTokens, identity?: UserIdentity): void {
    localStorage.setItem(ACCESS_TOKEN_KEY, tokens.access_token);
    localStorage.setItem(REFRESH_TOKEN_KEY, tokens.refresh_token);
    window.__TILED_ACCESS_TOKEN__ = tokens.access_token;
    if (identity) {
      localStorage.setItem(IDENTITY_KEY, JSON.stringify(identity));
    }
  }

  getAccessToken(): string | null {
    return localStorage.getItem(ACCESS_TOKEN_KEY);
  }

  getRefreshToken(): string | null {
    return localStorage.getItem(REFRESH_TOKEN_KEY);
  }

  getIdentity(): UserIdentity | null {
    try {
      return JSON.parse(localStorage.getItem(IDENTITY_KEY) || "null");
    } catch {
      return null;
    }
  }

  clearTokens(): void {
    localStorage.removeItem(ACCESS_TOKEN_KEY);
    localStorage.removeItem(REFRESH_TOKEN_KEY);
    localStorage.removeItem(IDENTITY_KEY);
    window.__TILED_ACCESS_TOKEN__ = null;
  }

  hasTokens(): boolean {
    return !!this.getAccessToken();
  }

  isAccessTokenExpired(): boolean {
    const token = this.getAccessToken();
    if (!token) return true;
    const payload = decodeJWT(token);
    if (!payload?.exp) return true;
    return payload.exp * 1000 - Date.now() < TOKEN_EXPIRY_BUFFER_MS;
  }

  /** Milliseconds until the access token expires, or 0 if already expired. */
  getTimeUntilExpiry(): number {
    const token = this.getAccessToken();
    if (!token) return 0;
    const payload = decodeJWT(token);
    if (!payload?.exp) return 0;
    return Math.max(0, payload.exp * 1000 - Date.now());
  }
}

export const tokenManager = new TokenManager();
