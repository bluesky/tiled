import React, { useEffect, useState, useCallback, useRef } from "react";
import { AuthContext } from "./auth-context";
import { authService } from "./auth-api";
import { tokenManager } from "./token-manager";
import { AuthState, AuthConfig, AuthTokens } from "./types";
import { setupAuthInterceptor, setupRefreshInterceptor } from "../client";

export const AuthProvider: React.FC<{ children: React.ReactNode }> = ({
  children,
}) => {
  const [state, setState] = useState<AuthState>({
    isAuthenticated: false,
    isLoading: true,
    user: null,
    tokens: null,
    error: null,
  });

  const [authConfig, setAuthConfig] = useState<AuthConfig | null>(null);
  const refreshTimeoutRef = useRef<NodeJS.Timeout | null>(null);

  const logout = useCallback(async () => {
    try {
      if (state.tokens?.access_token) {
        await authService.logout(state.tokens.access_token);
      }
    } catch (error) {
      console.error(error);
    } finally {
      tokenManager.clearTokens();
      if (refreshTimeoutRef.current) {
        clearTimeout(refreshTimeoutRef.current);
      }
      setState({
        isAuthenticated: false,
        isLoading: false,
        user: null,
        tokens: null,
        error: null,
      });
    }
  }, [state.tokens]);

  const refreshTokens = useCallback(async () => {
    if (!state.tokens?.refresh_token) {
      throw new Error("No refresh token available");
    }

    try {
      const newTokens = await authService.refreshSession(
        state.tokens.refresh_token,
      );
      tokenManager.saveTokens(newTokens);

      setState((prev) => ({
        ...prev,
        tokens: newTokens,
      }));
    } catch (error) {
      await logout();
      throw error;
    }
  }, [state.tokens, logout]);

  useEffect(() => {
    setupAuthInterceptor(() => {
      const tokens = tokenManager.getTokens();
      if (tokens?.access_token) {
        return tokens.access_token;
      }
      return null;
    });

    setupRefreshInterceptor(
      () => {
        const tokens = tokenManager.getTokens();
        return tokens?.refresh_token || null;
      },
      async (refreshToken: string) => {
        const newTokens = await authService.refreshSession(refreshToken);
        return newTokens;
      },
      (tokens: AuthTokens) => {
        tokenManager.saveTokens(tokens);
        setState((prev) => ({
          ...prev,
          tokens,
        }));
      },
      () => {
        tokenManager.clearTokens();
        setState({
          isAuthenticated: false,
          isLoading: false,
          user: null,
          tokens: null,
          error: null,
        });
      },
    );
  }, []);

  const scheduleTokenRefresh = useCallback(
    (tokens: AuthTokens): void => {
      if (refreshTimeoutRef.current) {
        clearTimeout(refreshTimeoutRef.current);
      }

      if (!tokens.expires_in) {
        return;
      }

      const bufferTime = Math.min(
        5 * 60 * 1000,
        (tokens.expires_in * 1000) / 2,
      );
      const refreshIn = tokens.expires_in * 1000 - bufferTime;

      if (refreshIn <= 0) {
        return;
      }

      refreshTimeoutRef.current = setTimeout(async () => {
        try {
          await refreshTokens();
        } catch (error) {
          logout();
        }
      }, refreshIn);
    },
    [refreshTokens, logout],
  );

  useEffect(() => {
    const initAuth = async () => {
      try {
        const config = await authService.getAuthConfig();
        setAuthConfig(config);

        const tokens = tokenManager.getTokens();

        if (!tokens) {
          setState((prev) => ({
            ...prev,
            isLoading: false,
            isAuthenticated: false,
          }));
          return;
        }

        if (tokenManager.isAccessTokenExpired(tokens)) {
          try {
            const newTokens = await authService.refreshSession(
              tokens.refresh_token,
            );
            tokenManager.saveTokens(newTokens);

            const user = await authService.getCurrentUser(
              newTokens.access_token,
            );

            setState({
              isAuthenticated: true,
              isLoading: false,
              user,
              tokens: newTokens,
              error: null,
            });

            scheduleTokenRefresh(newTokens);
          } catch (error) {
            tokenManager.clearTokens();
            setState((prev) => ({
              ...prev,
              isLoading: false,
              isAuthenticated: false,
            }));
          }
        } else {
          const user = await authService.getCurrentUser(tokens.access_token);
          setState({
            isAuthenticated: true,
            isLoading: false,
            user,
            tokens,
            error: null,
          });

          scheduleTokenRefresh(tokens);
        }
      } catch (error) {
        setState((prev) => ({
          ...prev,
          isLoading: false,
          error: "Failed to initialize authentication",
        }));
      }
    };

    initAuth();

    return () => {
      if (refreshTimeoutRef.current) {
        clearTimeout(refreshTimeoutRef.current);
      }
    };
  }, [scheduleTokenRefresh]);

  const login = async (
    provider: string,
    username: string,
    password: string,
  ) => {
    try {
      setState((prev) => ({ ...prev, isLoading: true, error: null }));

      const tokens = await authService.loginWithPassword(
        provider,
        username,
        password,
      );

      tokenManager.saveTokens(tokens);

      const user = await authService.getCurrentUser(tokens.access_token);

      setState({
        isAuthenticated: true,
        isLoading: false,
        user,
        tokens,
        error: null,
      });

      scheduleTokenRefresh(tokens);
    } catch (error: any) {
      setState((prev) => ({
        ...prev,
        isLoading: false,
        error: error.message || "Login failed",
      }));
      throw error;
    }
  };

  return (
    <AuthContext.Provider
      value={{
        ...state,
        login,
        logout,
        refreshTokens,
        authConfig,
      }}
    >
      {children}
    </AuthContext.Provider>
  );
};
