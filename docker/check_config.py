#!/usr/bin/env python

import os
import sys
from tiled.config import parse_configs


def main():
    try:
        tiled_config_path = os.environ["TILED_CONFIG"]
    except KeyError:
        print("tiled configuration file must be specified via TILED_CONFIG environment variable. Exiting...")
        sys.exit(1)

    config = parse_configs(tiled_config_path)

    authentication = config.get("authentication", {})
    allow_anonymous_access = authentication.get("allow_anonymous_access", False)

    if not (allow_anonymous_access or
            ("secret_keys" in authentication or "TILED_SERVER_SECRET_KEYS" in os.environ) or
            ("single_user_api_key" in authentication or "TILED_SINGLE_USER_API_KEY" in os.environ)):
        print("tiled configuration must allow anonymous access or explicitly specify keys. Exiting...")
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
