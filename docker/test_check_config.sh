#!/bin/bash

# Run check_config.py against a variety of configurations.
# This script will exit with code 1 if any of the confgiurations do not
# succeed or fail as expected.

set -e

# Locate the directory that contains this script.
# Use this to make the script executable from any directory.
export DIR=$( cd "$(dirname "$0")" ; pwd -P )

# Expected to succeed (exit 0)
bash -c 'TILED_CONFIG=$DIR/test_configs/config_with_api_key.yml python $DIR/check_config.py'
bash -c 'TILED_CONFIG=$DIR/test_configs/config_with_secret_keys.yml python $DIR/check_config.py'
bash -c 'TILED_CONFIG=$DIR/test_configs/config_public_no_authenticator.yml python $DIR/check_config.py'

# Expected to fail (exit 1)
bash -c '! TILED_CONFIG=$DIR/test_configs/config_missing_api_key.yml python $DIR/check_config.py'
bash -c '! TILED_CONFIG=$DIR/test_configs/config_missing_secret_keys.yml python $DIR/check_config.py'
bash -c '! TILED_CONFIG=$DIR/test_configs/config_missing_secret_keys_public.yml python $DIR/check_config.py'
