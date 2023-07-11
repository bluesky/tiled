# Run a local OIDC provider

This example runs a [toy OIDC provider][] (the same authentication mechanism
used by e.g. Google and GitHub) but running locally with fake users. **Do not
use in production.**

1. From this directory, start the OIDC provider service in a container as
   follows.

```
docker run --rm -p 9000:9000 -v $(pwd):/config -e CONFIG_FILE=/config/oidc_provider_config.json -e USERS_FILE=/config/users.json docker.io/qlik/simple-oidc-provider:0.2.4
```

2. From a web browser or commandline, access
   `http://localhost:9000/certs`. This contains
   public keys which _reset every time the container is (re)started_.
   Fill them into the Tiled configuration in this directory, `config.yml`,
   under `public_keys:`.

3. Start a tiled server, providing the environment variables referenced in the
   config file. These values can be used exactly as is; they match the values
   in the file `oidc_provider_config.json` used by the OIDC provider.

   ```
   OIDC_BASE_URL=http://localhost:9000 OIDC_CLIENT_ID=example_client_id OIDC_CLIENT_SECRET=example_client_secret tiled serve config config.yml
   ```
4. Connect from the Python client and initiate log in.

   ```python
   from tiled.client import from_uri
   c = from_uri("http://localhost:8000")
   c.login()
   ```

5. This will show a URL and may automatically navigate a web browser to it.
   When prompted to log in to the OIDC provider, use

   ```
   Username: example@example.com
   Password: password
   ```

   This can be customized in the file `users.json` used by the OIDC provider.

6. Follow the prompts from there, and login should succeed.

[toy OIDC provider]: https://hub.docker.com/r/qlik/simple-oidc-provider/
