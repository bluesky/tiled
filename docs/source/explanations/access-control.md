# Access Control

Tiled supports basic access control. The primary use case in mind is
controlling users' access to data based on a particular field in a database
indicating the "proposal" or "data session" to which a given dataset belongs,
and resolving that via some external user managmenet system.

The server current supports authentication using OAuth2 with JWT, where
the underlying user management and authentication system is pluggable in
exactly the same fashion as JupyterHub. To start, as a proof of concept, two
"Authenticator" plugins are implemented. See
https://github.com/bluesky/tiled/issues/3 for discussion and plans.