import { createContext, useContext } from "react";
import { UserIdentity } from "./types";
import { components } from "../openapi_schemas";

export interface AuthContextType {
  /** Whether the server requires authentication. */
  authRequired: boolean;
  /** Authentication providers from the server's /api/v1/ response. */
  providers: components["schemas"]["AboutAuthenticationProvider"][];
  /** Whether the user is currently authenticated (has valid tokens). */
  isAuthenticated: boolean;
  /** Whether auth initialization is complete. */
  initialized: boolean;
  /** The authenticated user's identity (id + provider). */
  identity: UserIdentity | null;
  /** Called after successful login to store tokens and update state. */
  onLogin: (
    accessToken: string,
    refreshToken: string,
    identity?: UserIdentity,
  ) => void;
  /** Log out: revokes session server-side and clears local state. */
  onLogout: () => void;
}

const AuthContext = createContext<AuthContextType>({
  authRequired: false,
  providers: [],
  isAuthenticated: false,
  initialized: false,
  identity: null,
  onLogin: () => {},
  onLogout: () => {},
});

export function useAuth() {
  return useContext(AuthContext);
}

export { AuthContext };
