#!/bin/sh

set -e
./build.sh > /dev/null
./venv/bin/pytest -v "$@"

