"""Writes config/evening_route_stops.json: which stops each post-6PM route serves, keyed by its pre-6PM route.

The evening route change (Gold 67->57, Green 68->54, Orange 53->55) swaps a bus onto a different path with a different
stop list. TransLoc renumbers stop IDs per service variant, so the two are compared by stop NAME (which carries the
direction, e.g. "... (Northbound)"), not ID. bus_eta.route_change_hidden_stops uses this to stop predicting stops the bus
will no longer reach once it changes over.

Needs a snapshot of TransLoc's real route/stop membership taken from prod (the /v1/trip-planner/uts-graph endpoint can't
be used: outside a variant's hours it shows the sibling's borrowed stops). Prod keeps one per day:
  MSYS_NO_PATHCONV=1 fly ssh sftp get /data/vehicle_logs/<yyyymmdd>_routes.json <dest>
Usage: python scripts/build_evening_route_stops.py <routes.json>
"""
import json
import sys
from pathlib import Path

PAIRS = {"67": "57", "68": "54", "53": "55"}  # pre-6PM route id -> post-6PM route id
OUT = Path(__file__).resolve().parents[1] / "config" / "evening_route_stops.json"


def main() -> None:
    snap = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    pairs = {}
    for old, new in PAIRS.items():
        names = sorted({s["Name"] for s in snap["stops"] if new in [str(r) for r in s.get("RouteIds", [])]})
        if not names:
            raise SystemExit(f"snapshot has no stops for route {new}; not writing")
        pairs[old] = {"to": new, "served_names": names}
    OUT.write_text(json.dumps({"source": Path(sys.argv[1]).name, "pairs": pairs}, indent=2, ensure_ascii=False) + "\n",
                   encoding="utf-8")
    print(f"wrote {OUT}: " + ", ".join(f"{o}->{p['to']} ({len(p['served_names'])} stops)" for o, p in pairs.items()))


if __name__ == "__main__":
    main()
