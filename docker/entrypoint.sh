#!/usr/bin/env bash

/app/docker/check_config.py && exec gunicorn --config /app/docker/gunicorn_config.py
