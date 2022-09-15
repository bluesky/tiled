#!/usr/bin/env bash

DEFAULT_GUNICORN_CONF=/app/gunicorn_config.py
export GUNICORN_CONF=${GUNICORN_CONF:-$DEFAULT_GUNICORN_CONF}

/app/check_config.py && exec gunicorn --config $GUNICORN_CONF
