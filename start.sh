#!/bin/sh
set -e
# Ensure data directories are writable by appuser for persistence
DATA_DIRS=${DATA_DIRS:-/data}
for d in $(echo "$DATA_DIRS" | tr ':' ' '); do
  if [ -d "$d" ]; then
    chown -R appuser:appuser "$d" || true
  else
    mkdir -p "$d"
    chown appuser:appuser "$d"
  fi
done

if [ -n "$VEH_LOG_DIRS" ]; then
  for d in $(echo "$VEH_LOG_DIRS" | tr ':' ' '); do
    if [ -d "$d" ]; then
      chown -R appuser:appuser "$d" || true
    fi
  done
fi
# Preserve the environment (Fly.io secrets) when switching to appuser
exec su --preserve-environment appuser -c "exec python -m uvicorn app:app --host 0.0.0.0 --port ${PORT:-8080}"

