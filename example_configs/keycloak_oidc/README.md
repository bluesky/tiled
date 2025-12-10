# Running a Local Keycloak Instance for Authentication

This example demonstrates how to set up authentication using Keycloak (or any OAuth2-compliant provider). Two clients require authentication:
1. Tiled CLI (command-line client)
2. Tiled Web UI (FastAPI server and frontend)

## Tiled CLI Authentication

The Tiled CLI uses the [device authorization flow](https://auth0.com/docs/get-started/authentication-and-authorization-flow/device-authorization-flow):

```mermaid
sequenceDiagram
    actor User
    participant CLI as Tiled CLI
    participant Server as Tiled Server
    participant IdP as Keycloak

    User->>CLI: client.login()
    CLI->>Server: Request auth configuration (/api/v1)
    Server-->>CLI: Device flow endpoints (client_id, auth-endpoint, token_endpoint)
    CLI->>IdP: POST /device (client_id, scopes)
    IdP-->>CLI: device_code, user_code, verification_uri, interval
    CLI->>User: "Visit verification URL and enter user code"

    par User Authentication
        User->>IdP: Open verification URL and authenticate
        IdP-->>User: Login successful
    and CLI Polling
        CLI->>IdP: Poll /token with device_code
        IdP-->>CLI: "authorization_pending" (repeat until login)
    end

    IdP-->>CLI: access_token, refresh_token
    CLI->>CLI: Store tokens (~/.cache/tiled)
    CLI-->>User: "You have logged in with Proxied OIDC as external user."
```

After login, subsequent requests include the access token in the Authorization header. When the token expires (1-minute validity), the CLI automatically refreshes it. You must create a public client in Keycloak with OAuth 2.0 Device Authorization Grant enabled (named `tiled-cli` in this example).

## Tiled Web UI Authentication

The Tiled Web UI uses [OAuth2 Proxy](https://oauth2-proxy.github.io/oauth2-proxy/) as a reverse proxy to the Tiled server. OAuth2 Proxy handles all session management, including login, logout, and access token refresh.
The web server uses a confidential Keycloak client (named `tiled` in this example).
To logout, users are redirected to the Keycloak end_session_endpoint via OAuth2 Proxy's sign-out handler:

```
http://localhost:4180/oauth2/sign_out?rd=http%3A%2F%2Flocalhost%3A8080%2Frealms%2Fmaster%2Fprotocol%2Fopenid-connect%2Flogout

# The logout URL can be build as follow:-
end_session_endpoint = "http://localhost:8080/realms/master/protocol/openid-connect/logout"
# end_session_endpoint can be found at http://localhost:8080/realms/master/.well-known/openid-configuration
encoded_url = urllib.parse.quote_plus(end_session_endpoint)

logout_url= "http://localhost:4180" + "oauth2/sign_out?rd=" + encoded_url
```

For better user experience, you can configure a `/logout` endpoint to redirect to this URL automatically.

```mermaid
sequenceDiagram
    actor User
    participant OAuth2Proxy as OAuth2 Proxy
    participant Keycloak
    participant Tiled

    User->>OAuth2Proxy: Request access to application
    OAuth2Proxy->>Keycloak: Redirect user for authentication
    activate Keycloak
    Keycloak-->>OAuth2Proxy: Return JWT Access Token
    deactivate Keycloak
    OAuth2Proxy->>Tiled: Forward request with JWT Access Token
    Tiled->>User: Provide resources if authenticated
```

## Getting Started

1. Run `docker compose up` in this directory. This starts:
   - **Keycloak**: Identity Provider (IdP)
   - **OAuth2-proxy**: Reverse-proxy to access OAuth2 secured Tiled

2. Start the Tiled server with `tiled server config example_configs/keycloak_oidc/config.yaml`. Tiled will make a call to Keycloak at startup to get the .well-known/openid-configuration.

3. Open http://localhost:4180 (OAuth2 proxy address) in your browser and log in with:
   - Username: `admin`
   - Password: `admin`

4. After authentication, you'll access all resources. Three additional test users are also available:
   - **alice** (password: alice)
   - **bob** (password: bob)
   - **carol** (password: carol)

> **Note:** This example exposes secrets and passwords for demonstration only. **Do not use in production.**
