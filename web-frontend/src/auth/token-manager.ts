import { AuthTokens } from "./types";

const TOKEN_KEY = "tiled_tokens";
const TOKEN_EXPIRY_BUFFER_MS = 60 * 1000; // 60 seconds

interface JWTPayload {
  exp?: number;
  iat?: number;
  sub?: string;
  [key: string]: unknown;
}

class TokenManager {
  saveTokens(tokens: AuthTokens): void {
    sessionStorage.setItem(TOKEN_KEY, JSON.stringify(tokens));
  }

  getTokens(): AuthTokens | null {
    const tokensJson = sessionStorage.getItem(TOKEN_KEY);
    return tokensJson ? JSON.parse(tokensJson) : null;
  }

  clearTokens(): void {
    sessionStorage.removeItem(TOKEN_KEY);
  }

  isAccessTokenExpired(tokens: AuthTokens): boolean {
    const payload = this.decodeToken(tokens.access_token);
    if (!payload?.exp) return true;

    const expirationTime = payload.exp * 1000;
    const timeUntilExpiry = expirationTime - Date.now();

    return timeUntilExpiry < TOKEN_EXPIRY_BUFFER_MS;
  }

  private decodeToken(token: string): JWTPayload | null {
    try {
      const [, payload] = token.split(".");
      if (!payload) return null;

      const decoded = atob(payload.replace(/-/g, "+").replace(/_/g, "/"));
      return JSON.parse(decoded);
    } catch {
      return null;
    }
  }
}

export const tokenManager = new TokenManager();
