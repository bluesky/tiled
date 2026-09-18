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
  // Whether the startup check for an existing server-side session (e.g. an
  // HttpOnly API-key cookie set by the server via ?api_key=...) has completed.
  // Until this is true, we must not let RequireAuth redirect to /login, or a
  // cookie-authenticated user would be bounced to the login page on load.
  const [cookieChecked, setCookieChecked] = useState(false);
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

  // On startup, establish the initial authentication state. This considers
  // three sources, in priority order:
  //   1. A valid (unexpired) local access token — already authenticated.
  //   2. An expired local access token — try to refresh it; if the refresh
  //      fails, fall through to (3) in case a cookie session exists.
  //   3. An HttpOnly API-key cookie set by the server when a URL with
  //      ?api_key=... is opened. JavaScript cannot read that cookie directly,
  //      so we ask the server: on multi-user servers via /auth/whoami (HTTP 200
  //      with a Principal body when authenticated, `null` when not); on
  //      single-user servers, which have no /auth routes, by probing a
  //      protected endpoint. See probeCookieSession.
  //
  // `initialized` is gated on this completing (via cookieChecked) so that
  // RequireAuth does not redirect a cookie-authenticated user to /login while
  // the check is still in flight.
  useEffect(() => {
    // Wait until the server's auth config is known.
    if (authentication === null) return;
    let mounted = true;

    // Detect a cookie-based session. Only called when there is no usable local
    // access token, so the request interceptor does not attach a stale Bearer
    // header that could mask the cookie on the server.
    //
    // Multi-user servers expose /auth/whoami, which returns the Principal
    // (including its identity) for a cookie-authenticated request, or `null`
    // when the request is not authenticated. Single-user (API-key) servers have
    // no authentication providers and therefore no /auth routes at all, so
    // whoami is absent (404). There we detect a cookie session by probing a
    // protected endpoint; a single-user principal has no identity to display.
    const hasProviders = (authentication.providers ?? []).length > 0;
    const probeCookieSession = async () => {
      if (hasProviders) {
        try {
          const response = await axiosInstance.get("/api/v1/auth/whoami");
          if (!mounted) return;
          const principal = response.data;
          if (principal) {
            const ident = principal.identities?.[0];
            if (ident) {
              setIdentity({ id: ident.id, provider: ident.provider });
            }
            setIsAuthenticated(true);
          }
        } catch {
          // Not authenticated via cookie (or request failed) — leave as-is.
        }
        return;
      }
      // Single-user mode: no /auth routes. A 200 from a protected endpoint
      // means the API-key cookie authenticated us; a 401 means it did not.
      try {
        await axiosInstance.get("/api/v1/metadata/");
        if (!mounted) return;
        setIsAuthenticated(true);
      } catch {
        // Not authenticated via cookie (or request failed) — leave as-is.
      }
    };

    const startup = async () => {
      // If the server does not require auth, there is nothing to detect.
      if (!authentication.required) return;
      if (tokenManager.hasTokens()) {
        if (tokenManager.isAccessTokenExpired()) {
          const refreshed = await refreshOnce();
          if (refreshed) {
            scheduleProactiveRefresh();
          } else {
            // Refresh failed and tokens were cleared; a cookie session may
            // still authenticate us (e.g. a freshly opened ?api_key=... link).
            await probeCookieSession();
          }
        } else {
          scheduleProactiveRefresh();
        }
      } else {
        await probeCookieSession();
      }
    };

    startup().finally(() => {
      if (mounted) setCookieChecked(true);
    });

    return () => {
      mounted = false;
      if (refreshTimeoutRef.current) {
        clearTimeout(refreshTimeoutRef.current);
      }
    };
  }, [authentication, refreshOnce, scheduleProactiveRefresh]);

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
        initialized: authentication !== null && cookieChecked,
        identity,
        onLogin,
        onLogout,
      }}
    >
      {children}
    </AuthContext.Provider>
  );
};
