#!/bin/sh
set -e
# Ensure /data is writable by appuser for persistence
if [ -d /data ]; then
  chown -R appuser:appuser /data || true
else
  mkdir -p /data
  chown appuser:appuser /data
fi
exec su appuser -c "exec python -m uvicorn app:app --host 0.0.0.0 --port ${PORT:-8080}"

