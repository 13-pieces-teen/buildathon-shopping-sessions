#!/bin/sh
set -eu
exec /app/shopping-sessions "${DATA_DIR:-/data}" "${OUTPUT_DIR:-/output}"
