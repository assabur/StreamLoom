#!/bin/sh
set -eu
superset db upgrade
python /app/streamloom/bootstrap.py
superset init
