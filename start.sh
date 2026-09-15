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

# Join the home-LAN Tailnet to reach the self-hosted Valhalla router (see
# ROUTING_ENGINE.md). Fly Machines don't grant a real tun device, so this runs in
# userspace-networking mode with a local outbound HTTP proxy -- trip_planner.py's
# WALK_ROUTER_PROXY_URL points at it. Skipped entirely (app behaves exactly as before)
# when TS_AUTHKEY isn't set, and any failure here is non-fatal to app startup.
if [ -n "$TS_AUTHKEY" ]; then
  # State lives on the persistent /data volume, not the container's ephemeral root fs --
  # otherwise every machine restart wipes the login and a non-reusable auth key can only
  # ever work for the very first boot (confirmed live: the very next restart came back
  # "Logged out").
  mkdir -p /data/tailscale
  /usr/sbin/tailscaled --tun=userspace-networking --socks5-server=localhost:1055 \
    --outbound-http-proxy-listen=localhost:1055 --state=/data/tailscale/tailscaled.state \
    >/var/log/tailscaled.log 2>&1 &
  tailscale up --authkey="$TS_AUTHKEY" --hostname="${FLY_MACHINE_ID:-uts-headway-guard}" \
    --accept-dns=false --timeout=20s || echo "[start.sh] tailscale up failed, continuing without walk router"
fi

# Preserve the environment (Fly.io secrets) when switching to appuser
exec su --preserve-environment appuser -c "exec python -m uvicorn app:app --host 0.0.0.0 --port ${PORT:-8080}"

