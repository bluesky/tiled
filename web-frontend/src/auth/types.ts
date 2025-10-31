export interface AuthTokens {
  access_token: string;
  refresh_token: string;
  expires_in: number;
  refresh_token_expires_in: number;
  token_type: "bearer";
}

export interface AuthProvider {
  provider: string;
  mode: "internal" | "external";
  links: {
    auth_endpoint: string;
  };
  confirmation_message?: string;
}

export interface AuthConfig {
  required: boolean;
  providers: AuthProvider[];
  links: {
    whoami: string;
    apikey: string;
    refresh_session: string;
    revoke_session: string;
    logout: string;
  };
}

export interface User {
  uuid: string;
  id: string;
  type: string;
  identities?: Array<{
    id: string;
    provider: string;
    latest_login?: string;
  }>;
  roles?: string[];
}

export interface AuthState {
  isAuthenticated: boolean;
  isLoading: boolean;
  user: User | null;
  tokens: AuthTokens | null;
  error: string | null;
}
