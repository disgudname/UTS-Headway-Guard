#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PORT1=4101
PORT2=4102
DATA1="$(mktemp -d)"
DATA2="$(mktemp -d)"
LOG1="$DATA1/server.log"
LOG2="$DATA2/server.log"
PID1=""
PID2=""

cleanup() {
  set +e
  if [[ -n "$PID1" ]]; then kill "$PID1" 2>/dev/null; wait "$PID1" 2>/dev/null; fi
  if [[ -n "$PID2" ]]; then kill "$PID2" 2>/dev/null; wait "$PID2" 2>/dev/null; fi
  if [[ -z "${KEEP_DATA:-}" ]]; then
    rm -rf "$DATA1" "$DATA2"
  else
    echo "Preserving test data in $DATA1 and $DATA2" >&2
  fi
}
trap cleanup EXIT

cd "$ROOT_DIR"

export NODE_ENV=test

wait_for() {
  local url="$1"
  local attempts=0
  until curl -sf "$url" >/dev/null; do
    attempts=$((attempts + 1))
    if [[ $attempts -ge 20 ]]; then
      echo "Timed out waiting for $url" >&2
      return 1
    fi
    sleep 0.5
  done
}
PORT=$PORT1 DATA_DIR="$DATA1" FLY_MACHINE_ID="machine-a" PEERS="http://127.0.0.1:$PORT2" node src/server.js >"$LOG1" 2>&1 &
PID1=$!
PORT=$PORT2 DATA_DIR="$DATA2" FLY_MACHINE_ID="machine-b" PEERS="http://127.0.0.1:$PORT1" node src/server.js >"$LOG2" 2>&1 &
PID2=$!

wait_for "http://127.0.0.1:$PORT1/health"
wait_for "http://127.0.0.1:$PORT2/health"

echo "Creating ticket on machine A"
CREATE_PAYLOAD='{
  "vehicle_name": "Bus 42",
  "vehicle_type": "bus",
  "reported_at": "2024-01-01T12:00:00Z",
  "reported_by": "operator",
  "ops_status": "DOWNED",
  "ops_description": "engine",
  "shop_status": "DOWN"
}'
CREATE_RES=$(curl -sf -X POST "http://127.0.0.1:$PORT1/api/tickets" -H 'Content-Type: application/json' -d "$CREATE_PAYLOAD")
TICKET_ID=$(node -e "const res = JSON.parse(process.argv[1]); process.stdout.write(res.id || res.ticket_id || res.ticket?.id || '');" "$CREATE_RES")
if [[ -z "$TICKET_ID" ]]; then
  echo "Ticket creation failed" >&2
  exit 1
fi

sleep 2

echo "Verifying replication to machine B"
B_LIST=$(curl -sf "http://127.0.0.1:$PORT2/api/tickets")
node -e "const list = JSON.parse(process.argv[1]); if (!Array.isArray(list) || !list.find((t) => t.id === process.argv[2])) { process.stderr.write('Ticket not replicated to machine B\n'); process.exit(1); }" "$B_LIST" "$TICKET_ID"

echo "Updating ticket on machine B"
UPDATE_PAYLOAD='{"shop_status": "UP", "mechanic": "tech"}'
curl -sf -X PUT "http://127.0.0.1:$PORT2/api/tickets/$TICKET_ID" -H 'Content-Type: application/json' -d "$UPDATE_PAYLOAD" >/dev/null
sleep 2

echo "Confirming machine A reflects update"
A_TICKET=$(curl -sf "http://127.0.0.1:$PORT1/api/tickets?includeClosed=true")
node -e "const list = JSON.parse(process.argv[1]); const item = list.find((t) => t.id === process.argv[2]); if (!item || item.shop_status !== 'UP' || item.mechanic !== 'tech') { process.stderr.write('Update not replicated\n'); process.exit(1); }" "$A_TICKET" "$TICKET_ID"

echo "Testing soft purge"
SOFT_PAYLOAD='{"start": "2023-12-31T00:00:00Z", "end": "2024-12-31T23:59:59Z", "vehicles": ["Bus 42"], "hard": false}'
curl -sf -X POST "http://127.0.0.1:$PORT1/api/purge" -H 'Content-Type: application/json' -d "$SOFT_PAYLOAD" >/dev/null
sleep 2
SIGNAGE=$(curl -sf "http://127.0.0.1:$PORT2/api/signage")
node -e "const list = JSON.parse(process.argv[1]); if (Array.isArray(list) && list.length !== 0) { process.stderr.write('Soft purge failed to hide ticket\n'); process.exit(1); }" "$SIGNAGE"

echo "Testing hard purge"
HARD_PAYLOAD='{"start": "2023-12-31T00:00:00Z", "end": "2024-12-31T23:59:59Z", "vehicles": ["Bus 42"], "hard": true}'
curl -sf -X POST "http://127.0.0.1:$PORT2/api/purge" -H 'Content-Type: application/json' -d "$HARD_PAYLOAD" >/dev/null
sleep 3

POST_PURGE=$(curl -sf "http://127.0.0.1:$PORT1/api/tickets?includeClosed=true")
node -e "const list = JSON.parse(process.argv[1]); if (Array.isArray(list) && list.find((t) => t.id === process.argv[2])) { process.stderr.write('Hard purge left ticket in state\n'); process.exit(1); }" "$POST_PURGE" "$TICKET_ID"

CSV=$(curl -sf "http://127.0.0.1:$PORT1/api/export.csv?start=2023-01-01T00:00:00Z&end=2025-01-01T00:00:00Z")
HEADER=$(printf '%s\n' "$CSV" | head -n 1)
if [[ "$HEADER" != "vehicle,ticket_id,reported_at,reported_by,ops_status,ops_description,shop_status,mechanic,diagnosis_text,started_at,completed_at,legacy_row_index,legacy_source,created_at,updated_at" ]]; then
  echo "CSV header mismatch" >&2
  exit 1
fi

META_PENDING=$(node -e "const fs=require('fs'); try { const meta=JSON.parse(fs.readFileSync(process.argv[1],'utf8')); const pending=meta.pending||{}; process.stdout.write(String(Object.keys(pending).length)); } catch(e){ process.stdout.write('0'); }" "$DATA1/meta.json")
if [[ "$META_PENDING" -ne 0 ]]; then
  echo "Pending replication entries remain on machine A" >&2
  exit 1
fi

echo "All acceptance checks passed"
