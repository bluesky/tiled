#!/usr/bin/env bash

./check_config.py && exec gunicorn --config /config/gunicorn_config.py
