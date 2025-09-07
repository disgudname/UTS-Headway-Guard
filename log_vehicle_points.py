import time, json
from pathlib import Path
import httpx

API_URL = "https://uva.transloc.com/Services/JSONPRelay.svc/GetMapVehiclePoints?APIKey=8882812681&returnVehiclesNotAssignedToRoute=true"
LOG_FILE = Path("vehicle_log.jsonl")
INTERVAL_SEC = 4
ONE_WEEK_MS = 7 * 24 * 3600 * 1000


def prune_old_entries():
    cutoff = int(time.time() * 1000) - ONE_WEEK_MS
    if not LOG_FILE.exists():
        return
    lines: list[str] = []
    with LOG_FILE.open() as f:
        for line in f:
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            if entry.get("ts", 0) >= cutoff:
                lines.append(line)
    with LOG_FILE.open("w") as f:
        f.writelines(lines)

def main():
    with httpx.Client(timeout=20) as client:
        while True:
            ts = int(time.time()*1000)
            r = client.get(API_URL)
            r.raise_for_status()
            data = r.json()
            vehicles = data if isinstance(data, list) else data.get("d", [])
            entry = {"ts": ts, "vehicles": vehicles}
            with LOG_FILE.open("a") as f:
                f.write(json.dumps(entry) + "\n")
            prune_old_entries()
            time.sleep(INTERVAL_SEC)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
