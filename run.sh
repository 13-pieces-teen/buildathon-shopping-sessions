#!/bin/sh
set -eu
export HOME="${SCRATCH_DIR:-/scratch}"
export TMPDIR="$HOME"
export PYTHONDONTWRITEBYTECODE=1
columns=$(mktemp -d "$HOME/columns.XXXXXXXX")
/app/shopping-sessions "${DATA_DIR:-/data}" "$columns"
python /app/write_parquet.py "$columns" "${OUTPUT_DIR:-/output}"
rm -f "$columns"/*.bin
rmdir "$columns"
