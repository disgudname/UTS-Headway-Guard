"""Predictions we published for a stop visit that never happened ("phantoms"), from one eta_watch log.

The normal scorer only sees predictions whose real crossing was observed later, so it cannot see a phantom: a bus
predicted at a stop it then never reaches (it changed route, went out of service, or was sent to the lot). Counts, per
minute of the run, predictions due within HORIZON_S that should have resolved before the log ended but didn't.
Usage: python scripts/eta_phantoms.py data-local/eta_watch/<log>.jsonl
"""
import collections
import datetime
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import eta_compare  # noqa: E402

HORIZON_S = 600.0   # only judge predictions that said "within 10 min"
MARGIN_S = 240.0    # ...and only if the log ran at least this long past the predicted arrival


def main():
    log = sys.argv[1]
    polls = [json.loads(l) for l in open(log, encoding="utf-8") if l.strip()]
    stops, lines = eta_compare.load_graph(log)
    resolved = {(r["t"], r["key"]) for r in eta_compare.score_rows(polls, stops, lines) if r["ours"] is not None}
    end = polls[-1]["t"]
    judged = collections.Counter()
    phantom = collections.Counter()
    who = collections.Counter()
    for p in polls:
        minute = datetime.datetime.fromtimestamp(p["t"]).strftime("%H:%M")
        for route, stop, veh, secs, *_ in p["ours"]:
            if secs > HORIZON_S or p["t"] + secs + MARGIN_S > end:
                continue
            judged[minute] += 1
            if (p["t"], (route, stop, veh)) not in resolved:
                phantom[minute] += 1
                who[(route, veh)] += 1
    print("minute  phantoms / judged")
    for m in sorted(judged):
        print(f"{m}   {phantom[m]:4d} / {judged[m]:4d}  {100.0 * phantom[m] / judged[m]:5.1f}%")
    tot_j, tot_p = sum(judged.values()), sum(phantom.values())
    print(f"\ntotal {tot_p} / {tot_j} = {100.0 * tot_p / max(1, tot_j):.1f}%")
    print("by (route, bus):", who.most_common(8))


if __name__ == "__main__":
    main()
