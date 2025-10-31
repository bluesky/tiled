// auth/tokenManager.ts
import { AuthTokens } from "./types";

const TOKEN_KEY = "tiled_tokens";

export const tokenManager = {
  /**
   * Save tokens to sessionStorage (more secure than localStorage)
   * Tokens are cleared when browser closes
   */
  saveTokens(tokens: AuthTokens): void {
    try {
      sessionStorage.setItem(TOKEN_KEY, JSON.stringify(tokens));
    } catch (error) {
      console.error("Failed to save tokens:", error);
    }
  },

  /**
   * Retrieve tokens from storage
   */
  getTokens(): AuthTokens | null {
    try {
      const tokensJson = sessionStorage.getItem(TOKEN_KEY);
      if (!tokensJson) return null;
      return JSON.parse(tokensJson) as AuthTokens;
    } catch (error) {
      console.error("Failed to retrieve tokens:", error);
      return null;
    }
  },

  /**
   * Remove tokens from storage for logout
   */
  clearTokens(): void {
    sessionStorage.removeItem(TOKEN_KEY);
  },

  /**
   * Check if access token is expired or about to expire
   * Returns true if token will expire in less than 60 seconds
   */
  isAccessTokenExpired(tokens: AuthTokens): boolean {
    if (!tokens.access_token) return true;

    try {
      // Decode JWT to get expiration time
      const payload = this.decodeToken(tokens.access_token);
      if (!payload.exp) return true;

      const expirationTime = payload.exp * 1000; // Convert to milliseconds
      const currentTime = Date.now();
      const bufferTime = 60 * 1000; // 60 second buffer

      return expirationTime - currentTime < bufferTime;
    } catch (error) {
      console.error("Failed to check token expiration:", error);
      return true;
    }
  },

  /**
   * Decode JWT token (without verification - server verifies)
   */
  decodeToken(token: string): any {
    try {
      const base64Url = token.split(".")[1];
      const base64 = base64Url.replace(/-/g, "+").replace(/_/g, "/");
      const jsonPayload = decodeURIComponent(
        atob(base64)
          .split("")
          .map((c) => "%" + ("00" + c.charCodeAt(0).toString(16)).slice(-2))
          .join(""),
      );
      return JSON.parse(jsonPayload);
    } catch (error) {
      console.error("Failed to decode token:", error);
      return null;
    }
  },
};
