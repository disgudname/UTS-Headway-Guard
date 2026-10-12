"""Put the digitized 2018 floor plan of Parking & Transit (1101 Millmont St) on the map, as GeoJSON.

The plan itself is NOT in this repo (the repo is public, the plan is for signed-in dispatchers only). This
script reads the digitized drawing (`old_floorplan_rooms.csv` + `old_floorplan_walls.json`, in feet, origin at
the building's north-west corner, y down) and writes one GeoJSON file that `GET /v1/livemap/floorplan` serves
from the data dir:

  python scripts/build_pt_floorplan.py "<folder with the digitized plan>" data-local/pt_floorplan_2018.geojson

The plan is placed with a rotation and a shift only (no scaling): plan feet -> Virginia State Plane South feet
(EPSG:2284). PLAN_TO_STATE_PLANE was fitted by laying the plan's outline over UVA's own footprint of the
building (facility 0583, PublicFacilitiesSearch layer); the outline sits a median of 0.4 ft from it.

Needs shapely and pyproj.
"""
import csv
import json
import math
import sys
from pathlib import Path

from pyproj import Transformer
from shapely import wkt
from shapely.geometry import LineString, Polygon, mapping
from shapely.ops import polylabel, transform, unary_union

# rotation (radians, counter-clockwise) and the State Plane position of the plan's origin
PLAN_TO_STATE_PLANE = (0.882014, 11481552.654, 3906425.652)

_TO_LONLAT = Transformer.from_crs("EPSG:2284", "EPSG:4326", always_xy=True)


def to_lonlat(x, y, z=None):
    th, tx, ty = PLAN_TO_STATE_PLANE
    c, s = math.cos(th), math.sin(th)
    lon, lat = _TO_LONLAT.transform(tx + c * x + s * y, ty + s * x - c * y)   # y is down on the plan
    return round(lon, 7), round(lat, 7)


def feature(geom, **props):
    return {"type": "Feature", "properties": props, "geometry": mapping(transform(to_lonlat, geom))}


def build(src: Path):
    feats, rooms = [], []
    with open(src / "old_floorplan_rooms.csv", newline="") as f:
        for row in csv.DictReader(f):
            key = next(k for k in row if k.startswith("polygon_ft"))
            poly = Polygon([tuple(map(float, p.split(","))) for p in row[key].split()])
            rooms.append(poly)
            name, number = row["name"].strip(), row["room_number"].strip()
            label = name if name == number or not number else f"{name}\n{number}"
            props = dict(name=name, room=number, dept=row["department"].strip(), sf=round(poly.area), label=label)
            feats.append(feature(poly, kind="room", **props))
            feats.append(feature(polylabel(poly, tolerance=0.2), kind="label", **props))
    w = json.load(open(src / "old_floorplan_walls.json"))
    walls = unary_union([wkt.loads(w["walls"]), wkt.loads(w["thick"])])
    # one plain floor under everything, so corridors (not rooms on the drawing) do not show the roof
    floor = unary_union(rooms + [walls]).buffer(6).buffer(-6)
    feats.insert(0, feature(floor, kind="floor"))
    feats.append(feature(walls, kind="wall"))
    for line in w.get("partitions", []):
        feats.append(feature(LineString(line), kind="partition"))
    return {"type": "FeatureCollection", "name": "Parking & Transit, 2018 floor plan", "features": feats}


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    out = Path(sys.argv[2])
    out.parent.mkdir(parents=True, exist_ok=True)
    fc = build(Path(sys.argv[1]))
    out.write_text(json.dumps(fc, separators=(",", ":")), encoding="utf-8")
    print(f"{out}: {len(fc['features'])} features, {out.stat().st_size / 1024:.0f} KB")
