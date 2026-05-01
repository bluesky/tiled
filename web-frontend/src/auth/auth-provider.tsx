import React, { useEffect, useState, useRef, useCallback } from "react";
import axios, { AxiosError, InternalAxiosRequestConfig } from "axios";
import { AuthContext } from "./auth-context";
import { tokenManager } from "./token-manager";
import { UserIdentity } from "./types";
import { axiosInstance } from "../client";
import { components } from "../openapi_schemas";

interface AuthProviderProps {
  /** Authentication config from the server's GET /api/v1/ response. Null while loading. */
  authentication: components["schemas"]["AboutAuthentication"] | null;
  children: React.ReactNode;
}

/**
 * Provides authentication state and token lifecycle management.
 *
 * - Sets up axios interceptors (request: attach Bearer token, response: 401 → refresh)
 * - Schedules proactive token refresh before expiry
 * - Cleans up interceptors on unmount
 */
export const AuthProvider: React.FC<AuthProviderProps> = ({
  authentication,
  children,
}) => {
  const [isAuthenticated, setIsAuthenticated] = useState(() =>
    tokenManager.hasTokens(),
  );
  const [identity, setIdentity] = useState<UserIdentity | null>(() =>
    tokenManager.getIdentity(),
  );
  const refreshTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const refreshPromiseRef = useRef<Promise<boolean> | null>(null);

  // Shared refresh logic used by both proactive and reactive refresh.
  const doRefresh = useCallback(async (): Promise<boolean> => {
    const refreshToken = tokenManager.getRefreshToken();
    if (!refreshToken) return false;
    try {
      const resp = await axios.post("/api/v1/auth/session/refresh", {
        refresh_token: refreshToken,
      });
      tokenManager.saveTokens({
        access_token: resp.data.access_token,
        refresh_token: resp.data.refresh_token,
      });
      setIsAuthenticated(true);
      return true;
    } catch {
      tokenManager.clearTokens();
      setIsAuthenticated(false);
      setIdentity(null);
      return false;
    }
  }, []);

  // Deduplicated refresh: multiple 401s only trigger one refresh request.
  const refreshOnce = useCallback(async (): Promise<boolean> => {
    if (!refreshPromiseRef.current) {
      refreshPromiseRef.current = doRefresh().finally(() => {
        refreshPromiseRef.current = null;
      });
    }
    return refreshPromiseRef.current;
  }, [doRefresh]);

  // Schedule proactive refresh before the access token expires.
  const scheduleProactiveRefresh = useCallback(() => {
    if (refreshTimeoutRef.current) {
      clearTimeout(refreshTimeoutRef.current);
      refreshTimeoutRef.current = null;
    }
    const ttl = tokenManager.getTimeUntilExpiry();
    if (ttl <= 0) return;
    // Refresh at half the remaining time, but at most 5 minutes before expiry.
    const bufferMs = Math.min(5 * 60 * 1000, ttl / 2);
    const delay = ttl - bufferMs;
    if (delay <= 0) return;
    refreshTimeoutRef.current = setTimeout(async () => {
      const ok = await refreshOnce();
      if (ok) scheduleProactiveRefresh();
    }, delay);
  }, [refreshOnce]);

  // Set up axios interceptors (once, with cleanup).
  useEffect(() => {
    const requestId = axiosInstance.interceptors.request.use(
      (config: InternalAxiosRequestConfig) => {
        const token = tokenManager.getAccessToken();
        if (token) {
          config.headers.set("Authorization", `Bearer ${token}`);
        }
        return config;
      },
    );

    const responseId = axiosInstance.interceptors.response.use(
      (response) => response,
      async (error: AxiosError) => {
        const original = error.config as InternalAxiosRequestConfig & {
          _retry?: boolean;
        };
        if (
          error.response?.status === 401 &&
          original &&
          !original._retry
        ) {
          original._retry = true;
          if (tokenManager.getRefreshToken()) {
            const ok = await refreshOnce();
            if (ok) {
              const token = tokenManager.getAccessToken();
              if (token) {
                original.headers.set("Authorization", `Bearer ${token}`);
              }
              return axiosInstance(original);
            }
            // Refresh failed — reload to reach login page.
            window.location.reload();
          }
        }
        return Promise.reject(error);
      },
    );

    return () => {
      axiosInstance.interceptors.request.eject(requestId);
      axiosInstance.interceptors.response.eject(responseId);
    };
  }, [refreshOnce]);

  // On mount: refresh expired tokens; schedule proactive refresh for valid ones.
  useEffect(() => {
    if (!tokenManager.hasTokens()) return;
    if (tokenManager.isAccessTokenExpired()) {
      refreshOnce().then((ok) => {
        if (ok) scheduleProactiveRefresh();
      });
    } else {
      scheduleProactiveRefresh();
    }
    return () => {
      if (refreshTimeoutRef.current) {
        clearTimeout(refreshTimeoutRef.current);
      }
    };
  }, [refreshOnce, scheduleProactiveRefresh]);

  const onLogin = useCallback(
    (accessToken: string, refreshToken: string, ident?: UserIdentity) => {
      tokenManager.saveTokens(
        { access_token: accessToken, refresh_token: refreshToken },
        ident,
      );
      setIsAuthenticated(true);
      if (ident) setIdentity(ident);
      scheduleProactiveRefresh();
    },
    [scheduleProactiveRefresh],
  );

  const onLogout = useCallback(() => {
    tokenManager.clearTokens();
    setIsAuthenticated(false);
    setIdentity(null);
    if (refreshTimeoutRef.current) {
      clearTimeout(refreshTimeoutRef.current);
      refreshTimeoutRef.current = null;
    }
  }, []);

  return (
    <AuthContext.Provider
      value={{
        authRequired: authentication?.required ?? false,
        providers: authentication?.providers ?? [],
        isAuthenticated,
        initialized: authentication !== null,
        identity,
        onLogin,
        onLogout,
      }}
    >
      {children}
    </AuthContext.Provider>
  );
};
