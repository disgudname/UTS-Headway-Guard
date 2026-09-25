"""What the block [08] bus did around its 18:00 stadium departure, per weekday, from data-local/ridership/ (see
scripts/ridership_pull.py). Block [08]: "leave CSW at 1800, stay in service until MP, return to lot".

Per day it finds the bus(es) dispatch had on [08] (the day's /v1/servicecrew block map), then, from that bus's
door-counter events 17:30-18:40: who boarded at CSW (Carl Smith Way @ Scott Stadium) 17:57-18:05, where those riders
got off, and whether the bus kept running the Orange loop to Madison/Preston (MP) afterwards.
  python scripts/block8_ridership.py [--verbose]
"""
import datetime
import gzip
import json
import re
import sys
from pathlib import Path

DATA = Path(__file__).resolve().parents[1] / "data-local" / "ridership"
CSW = "Carl Smith Way @ Scott Stadium"
NEAR_CSW = ("Stadium Rd @ Alderman Rd", "Alderman Rd @ Gooch/Dillard")
MP = "Madison Ave @ Preston Ave"


def parse(s):
    return datetime.datetime.strptime(s, "%m/%d/%Y %I:%M:%S %p")


def orange(route):
    return "Orange" in route


def main():
    verbose = "--verbose" in sys.argv
    print(f"{'date':10} {'dow':3} {'bus':6} {'at CSW':>8} {'off@CSW':>7} {'boarded':>7} {'off<=6min':>9} {'at 2 stops':>10}  MP?  last Orange event")
    for path in sorted(DATA.glob("*.json.gz")):
        day = datetime.date.fromisoformat(path.name[:10])
        if day.weekday() >= 5:
            continue
        blocks_path = DATA / f"{day}.blocks.json"
        if not blocks_path.exists():
            print(day, "no block map")
            continue
        buses = json.loads(blocks_path.read_text())["buses"]
        bus_names = [b for b, v in buses.items() if any(re.search(r"\[08\]", x) for x in v["blocks"])]
        if not bus_names:
            print(f"{day} {day.strftime('%a')}  no bus on [08]")
            continue
        rows = json.loads(gzip.decompress(path.read_bytes()))
        rows = [r for r in rows if parse(r["ClientTime"]).date() == day]
        for bus in bus_names:
            ev = sorted((r for r in rows if r["Vehicle"] == bus), key=lambda r: parse(r["ClientTime"]))
            win = [r for r in ev if datetime.time(17, 30) <= parse(r["ClientTime"]).time() <= datetime.time(18, 40)]
            # The bus's last Orange visit to CSW between 17:50 and 18:15 (cluster of events < 90 s apart): it arrives anywhere
            # from ~17:55 to ~18:08 depending on the day, so a fixed clock window misses some days.
            csw_ev = [r for r in ev if orange(r["Route"]) and r["RouteStop"] == CSW
                      and datetime.time(17, 50) <= parse(r["ClientTime"]).time() <= datetime.time(18, 15)]
            cluster = []
            for r in csw_ev:
                if cluster and (parse(r["ClientTime"]) - parse(cluster[-1]["ClientTime"])).total_seconds() >= 90:
                    cluster = []
                cluster.append(r)
            csw = cluster
            boarded = sum(r["Entries"] for r in csw)
            csw_out = sum(r["Exits"] for r in csw)
            if csw:
                t_csw = parse(csw[0]["ClientTime"])
                last_csw = parse(csw[-1]["ClientTime"])
                nxt = [r for r in ev if orange(r["Route"]) and last_csw < parse(r["ClientTime"]) <= last_csw + datetime.timedelta(minutes=6)
                       and r["RouteStop"] != CSW]
                off_near = sum(r["Exits"] for r in nxt if (r["RouteStop"] or "").startswith(NEAR_CSW))
                off_all = sum(r["Exits"] for r in nxt)
                after_csw_orange = [r for r in win if orange(r["Route"]) and parse(r["ClientTime"]) > last_csw]
            else:
                t_csw = None
                csw_out = off_near = off_all = 0
                after_csw_orange = [r for r in win if orange(r["Route"]) and parse(r["ClientTime"]).time() > datetime.time(18, 0, 30)]
            went_mp = any(r["RouteStop"] == MP and parse(r["ClientTime"]).time() > datetime.time(17, 58) for r in after_csw_orange)
            last = max((parse(r["ClientTime"]) for r in ev if orange(r["Route"]) and parse(r["ClientTime"]).time() < datetime.time(19, 30)
                        and parse(r["ClientTime"]).time() > datetime.time(17, 30)), default=None)
            print(f"{day} {day.strftime('%a')} {bus:6} {t_csw.strftime('%H:%M:%S') if t_csw else '-':>8} {csw_out:7d} {boarded:7d} {off_all:9d} {off_near:10d}  {'YES' if went_mp else 'no ':4} {last.strftime('%H:%M:%S') if last else '-'}")
            if verbose:
                groups = []
                for r in win:
                    t = parse(r["ClientTime"])
                    key = (r["RouteStop"] or "", r["Route"].strip())
                    if groups and groups[-1]["key"] == key and (t - groups[-1]["t1"]).total_seconds() < 60:
                        g = groups[-1]; g["in"] += r["Entries"]; g["out"] += r["Exits"]; g["t1"] = t
                    else:
                        groups.append({"key": key, "t0": t, "t1": t, "in": r["Entries"], "out": r["Exits"]})
                for g in groups:
                    if g["t0"].time() >= datetime.time(17, 50):
                        print(f"     {g['t0'].strftime('%H:%M:%S')} {g['key'][0][:36]:36} {g['key'][1][:14]:14} in={g['in']} out={g['out']}")


if __name__ == "__main__":
    main()
