#!/usr/bin/env python

import os
import sys

from tiled.config import parse_configs


def main():
    try:
        tiled_config_path = os.environ["TILED_CONFIG"]
    except KeyError:
        print(
            "tiled configuration file must be specified via TILED_CONFIG environment variable. Exiting...",
            file=sys.stderr,
        )
        sys.exit(1)

    config = parse_configs(tiled_config_path)

    authentication = config.get("authentication", {})
    allow_anonymous_access = authentication.get("allow_anonymous_access", False)

    if authentication.get("authenticator") is not None:
        # Even if the deployment allows public, anonymous access, secret
        # keys are needed to generate JWTs for any users that do log in.
        if not (
            ("secret_keys" in authentication)
            or ("TILED_SERVER_SECRET_KEYS" in os.environ)
        ):
            print(
                """
When Tiled is configured with an Authenticator, secret keys must
be provided via configuration like

authentication:
  secret_keys:
    - SECRET
  ...

or via the environment variable TILED_SERVER_SECRET_KEYS.  Exiting...""",
                file=sys.stderr,
            )
            sys.exit(1)
    else:
        # No authentication mechanism is configured, so no secret keys are
        # needed, but a single-user API key must be set unless the deployment
        # is public.
        if not (
            allow_anonymous_access
            or ("single_user_api_key" in authentication)
            or ("TILED_SINGLE_USER_API_KEY" in os.environ)
        ):
            print(
                """
When Tiled is configured for single-user access (i.e. without an
Authenticator), it must either be set to allow anonymous (public) access like

authentication:
  allow_anonymous_access: true
  ...

or else a single-user API key must be provided via configuration like

authentication:
  single_user_api_key: SECRET
  ...

or via the environment variable TILED_SINGLE_USER_API_KEY. Exiting...""",
                file=sys.stderr,
            )
            sys.exit(1)
    # If we reach here, the no configuration problems were found.
    sys.exit(0)


if __name__ == "__main__":
    main()
