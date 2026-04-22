import React, { createContext, useContext, useState } from "react";
import { components } from "../openapi_schemas";

const ACCESS_TOKEN_KEY = "tiled_access_token";
const REFRESH_TOKEN_KEY = "tiled_refresh_token";
const IDENTITY_KEY = "tiled_identity";

// Expose token globally so custom spec_view scripts can use it.
declare global {
  interface Window {
    __TILED_ACCESS_TOKEN__?: string | null;
  }
}

export function getStoredAccessToken(): string | null {
  return localStorage.getItem(ACCESS_TOKEN_KEY);
}

export function getStoredRefreshToken(): string | null {
  return localStorage.getItem(REFRESH_TOKEN_KEY);
}

export function storeTokens(
  accessToken: string,
  refreshToken: string,
  identity?: { id: string; provider: string },
) {
  localStorage.setItem(ACCESS_TOKEN_KEY, accessToken);
  localStorage.setItem(REFRESH_TOKEN_KEY, refreshToken);
  window.__TILED_ACCESS_TOKEN__ = accessToken;
  if (identity) {
    localStorage.setItem(IDENTITY_KEY, JSON.stringify(identity));
  }
}

export function clearTokens() {
  localStorage.removeItem(ACCESS_TOKEN_KEY);
  localStorage.removeItem(REFRESH_TOKEN_KEY);
  localStorage.removeItem(IDENTITY_KEY);
  window.__TILED_ACCESS_TOKEN__ = null;
}

// Initialize on module load
try {
  window.__TILED_ACCESS_TOKEN__ = localStorage.getItem(ACCESS_TOKEN_KEY);
} catch {
  // localStorage not available (e.g., in tests)
}

function getStoredIdentity(): { id: string; provider: string } | null {
  try {
    return JSON.parse(localStorage.getItem(IDENTITY_KEY) || "null");
  } catch {
    return null;
  }
}

export interface AuthState {
  authRequired: boolean;
  providers: components["schemas"]["AboutAuthenticationProvider"][];
  isAuthenticated: boolean;
  identity: { id: string; provider: string } | null;
  onLogin: (
    accessToken: string,
    refreshToken: string,
    identity?: { id: string; provider: string },
  ) => void;
  onLogout: () => void;
  initialized: boolean;
}

const AuthContext = createContext<AuthState>({
  authRequired: false,
  providers: [],
  isAuthenticated: false,
  identity: null,
  onLogin: () => {},
  onLogout: () => {},
  initialized: false,
});

export const useAuth = () => useContext(AuthContext);

export function AuthProvider({
  authentication,
  children,
}: {
  authentication: components["schemas"]["AboutAuthentication"] | null;
  children: React.ReactNode;
}) {
  const [isAuthenticated, setIsAuthenticated] = useState(
    () => !!getStoredAccessToken(),
  );
  const [identity, setIdentity] = useState(getStoredIdentity);

  const onLogin = (
    accessToken: string,
    refreshToken: string,
    ident?: { id: string; provider: string },
  ) => {
    storeTokens(accessToken, refreshToken, ident);
    setIsAuthenticated(true);
    if (ident) setIdentity(ident);
  };

  const onLogout = () => {
    clearTokens();
    setIsAuthenticated(false);
    setIdentity(null);
  };

  return (
    <AuthContext.Provider
      value={{
        authRequired: authentication?.required ?? false,
        providers: authentication?.providers ?? [],
        isAuthenticated,
        identity,
        onLogin,
        onLogout,
        initialized: authentication !== null,
      }}
    >
      {children}
    </AuthContext.Provider>
  );
}
