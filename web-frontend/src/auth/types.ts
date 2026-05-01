/**
 * Auth types for the Tiled web UI.
 */

export interface AuthTokens {
  access_token: string;
  refresh_token: string;
}

export interface UserIdentity {
  id: string;
  provider: string;
}
