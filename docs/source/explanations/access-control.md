# Access Control

Tiled supports basic access control. The primary use case in mind is
controlling users' access to data based on a particular field in a database
indicating the "proposal" or "data session" to which a given dataset belongs,
and resolving that via some external user managmenet system.

The server current supports simple API key based authentication as proof of
concept. See https://github.com/bluesky/tiled/issues/3 for discussion and
plans.