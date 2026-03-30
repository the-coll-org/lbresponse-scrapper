#!/bin/sh
set -e

# Ensure output directory and existing files are writable
# Needed for rootless Docker where UID mapping causes permission mismatches
mkdir -p /app/output
chmod -R a+rw /app/output 2>/dev/null || true

exec "$@"
