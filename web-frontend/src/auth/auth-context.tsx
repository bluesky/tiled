import { createContext, useContext } from "react";
import { AuthState, AuthConfig } from "./types";

interface AuthContextType extends AuthState {
  login: (
    provider: string,
    username: string,
    password: string,
  ) => Promise<void>;
  logout: () => Promise<void>;
  refreshTokens: () => Promise<void>;
  authConfig: AuthConfig | null;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

export function useAuth() {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error("useAuth must be used within AuthProvider");
  }
  return context;
}

export { AuthContext };
