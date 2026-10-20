"""GTFS-Realtime for UTS: vehicle positions, trip updates and service alerts, on top of our static feed (gtfs_feed.py).

Every id in these feeds is one /gtfs.zip uses: trip_id, route_id, stop_id (physical AddressID). Apps (Google, Transit,
OpenTripPlanner...) poll a GTFS-RT feed from their own servers every 15-30 s and hand it to their riders, so app.py only
rebuilds these every few seconds.

  * Vehicle positions: TransLoc's live bus list (GetMapVehiclePoints), each bus matched to the trip it is running.
  * Trip updates: our own ETA engine's predictions (/v1/eta/uts_stop_arrivals, bus_eta.py) for the stops left on that
    trip, and on the block's next trip when it stays on the same route.
  * Alerts: active system notices (/system-notices), targeted at the routes or stops the notice names.

Matching a bus to a trip: the bus's block (TransLoc dispatch, as the ETA engine already resolves it) narrows the
candidates to that block's trips on the bus's route that run today; the one whose scheduled time at the bus's next
stop is closest to the ETA engine's arrival there wins, within MATCH_EARLY_S / MATCH_LATE_S. Purple has no blocks in
the static feed: it is one frequency trip per headway band, and the bus's start_time (required for a frequency trip)
is worked back from its next stop.

Pure: app.py passes in the vehicles, ETAs, notices and clock.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from google.transit import gtfs_realtime_pb2 as pb

from gtfs_feed import FeedIndex, IndexedTrip, gtfs_time

# How far a bus may be from its trip's schedule and still be matched to it.
MATCH_EARLY_S = 15 * 60
MATCH_LATE_S = 30 * 60
# Without a known block, only a close match is trusted (two buses share a route).
MATCH_NO_BLOCK_S = 8 * 60

_SEVERITY = {"red": pb.Alert.SEVERE, "orange": pb.Alert.WARNING, "yellow": pb.Alert.WARNING}


@dataclass
class Vehicle:
    vehicle_id: str            # TransLoc VehicleID
    label: str                 # bus number
    transloc_route_id: Optional[str]
    lat: float
    lon: float
    bearing: Optional[float] = None
    speed_mps: Optional[float] = None
    timestamp: Optional[int] = None  # epoch seconds of the GPS fix
    block: Optional[str] = None      # "[09]" (dispatch); None when unknown


@dataclass
class Match:
    trip: IndexedTrip
    start_time: Optional[str]   # only for a frequency trip
    next_index: int             # position in trip.stops of the bus's next stop
    updates: List[Tuple[int, int]] = field(default_factory=list)  # (index into trip.stops, arrival epoch)


def etas_by_vehicle(payload: Dict[str, Any]) -> Dict[Tuple[str, str], List[Tuple[str, float]]]:
    """(vehicle id, TransLoc route id) -> [(route stop id, seconds)], soonest first, from the ETA engine's payload."""
    out: Dict[Tuple[str, str], List[Tuple[str, float]]] = {}
    for entry in (payload or {}).get("arrivals") or []:
        rid, rsid = str(entry.get("RouteId")), str(entry.get("RouteStopId"))
        for t in entry.get("Times") or []:
            if t.get("VehicleId") is None or t.get("Seconds") is None:
                continue
            out.setdefault((str(t["VehicleId"]), rid), []).append((rsid, float(t["Seconds"])))
    for v in out.values():
        v.sort(key=lambda x: x[1])
    return out


def blocks_by_vehicle(payload: Dict[str, Any]) -> Dict[str, str]:
    """vehicle id -> block id the ETA engine used for it."""
    out: Dict[str, str] = {}
    for entry in (payload or {}).get("arrivals") or []:
        for t in entry.get("Times") or []:
            if t.get("VehicleId") is not None and t.get("BlockId"):
                out[str(t["VehicleId"])] = str(t["BlockId"])
    return out


def _block_label(block: Optional[str]) -> Optional[str]:
    return block.strip().strip("[]") if block else None


def match_trip(
    index: FeedIndex, service_date: str, day_start: float, vehicle: Vehicle, etas: Sequence[Tuple[str, float]],
    now: float,
) -> Optional[Match]:
    """The trip this bus is running, or None. day_start: epoch of the service day's 00:00 (GTFS "noon minus 12h")."""
    if not etas or not vehicle.transloc_route_id:
        return None
    services = index.services_by_date.get(service_date) or set()
    next_rsid, next_s = etas[0]
    predicted = now + next_s
    label = _block_label(vehicle.block)
    best: Optional[Tuple[float, IndexedTrip, int]] = None
    for trip in index.trips.values():
        if trip.transloc_route_id != vehicle.transloc_route_id or trip.service_id not in services:
            continue
        if trip.frequency is None and label is not None and trip.block != label:
            continue
        last_k = len(trip.stops) - 1
        for k, stop in enumerate(trip.stops):
            if stop.route_stop_id != next_rsid:
                continue
            if trip.frequency is not None and k == last_k and k > 0:
                continue  # a Purple lap ends where it starts: a bus heading there is starting its next lap
            if trip.frequency is not None:
                start, end, _ = trip.frequency
                offset = stop.arrival_s - trip.stops[0].arrival_s
                begun = predicted - offset - day_start
                if not (start - 300 <= begun < end):
                    continue
                score = 0.0
            else:
                sched = day_start + (stop.departure_s if k == 0 else stop.arrival_s)
                diff = predicted - sched
                limit_early, limit_late = (MATCH_EARLY_S, MATCH_LATE_S) if label else (MATCH_NO_BLOCK_S, MATCH_NO_BLOCK_S)
                if not (-limit_early <= diff <= limit_late):
                    continue
                score = abs(diff)
            if best is None or score < best[0]:
                best = (score, trip, k)
    if best is None:
        return None
    _, trip, k = best
    start_time = None
    if trip.frequency is not None:
        start, end, _ = trip.frequency
        offset = trip.stops[k].arrival_s - trip.stops[0].arrival_s
        begun = int(round(predicted - offset - day_start))
        start_time = gtfs_time(min(max(begun, start), end - 1))
    match = Match(trip=trip, start_time=start_time, next_index=k)
    eta_at = {rsid: now + s for rsid, s in etas}
    last = now
    for m in range(k, len(trip.stops)):
        t = eta_at.get(trip.stops[m].route_stop_id)
        if t is None or t < last or any(trip.stops[m].route_stop_id == trip.stops[x].route_stop_id for x in range(k, m)):
            continue
        match.updates.append((m, int(round(t))))
        last = t
    return match


def next_block_trip(index: FeedIndex, service_date: str, trip: IndexedTrip) -> Optional[IndexedTrip]:
    """The block's next trip after this one, same service day."""
    if trip.block is None:
        return None
    services = index.services_by_date.get(service_date) or set()
    later = [
        t for t in index.trips.values()
        if t.block == trip.block and t.service_id == trip.service_id and t.service_id in services
        and t.stops and t.stops[0].departure_s >= trip.stops[-1].arrival_s and t.trip_id != trip.trip_id
    ]
    return min(later, key=lambda t: t.stops[0].departure_s) if later else None


def _header(msg: Any, now: float) -> None:
    msg.header.gtfs_realtime_version = "2.0"
    msg.header.incrementality = pb.FeedHeader.FULL_DATASET
    msg.header.timestamp = int(now)


def _trip_descriptor(desc: Any, trip: IndexedTrip, service_date: str, start_time: Optional[str]) -> None:
    desc.trip_id = trip.trip_id
    desc.route_id = trip.route_id
    desc.start_date = service_date
    if start_time:
        desc.start_time = start_time
    desc.schedule_relationship = pb.TripDescriptor.SCHEDULED


def build_realtime(
    index: FeedIndex, service_date: str, day_start: float, vehicles: Iterable[Vehicle],
    etas: Dict[Tuple[str, str], List[Tuple[str, float]]], now: float,
) -> Tuple[Any, Any, Dict[str, Any]]:
    """(VehiclePositions FeedMessage, TripUpdates FeedMessage, stats)."""
    positions, updates = pb.FeedMessage(), pb.FeedMessage()
    _header(positions, now)
    _header(updates, now)
    stats = {"vehicles": 0, "matched": 0, "unmatched": []}
    for v in sorted(vehicles, key=lambda v: v.vehicle_id):
        route_id = index.route_of.get(v.transloc_route_id or "")
        if route_id is None:
            continue  # not on a route the static feed has (out of service, a lot shuttle, Charter...)
        stats["vehicles"] += 1
        bus_etas = etas.get((v.vehicle_id, v.transloc_route_id or ""), [])
        match = match_trip(index, service_date, day_start, v, bus_etas, now)
        ent = positions.entity.add()
        ent.id = f"vehicle-{v.vehicle_id}"
        vp = ent.vehicle
        if match:
            stats["matched"] += 1
            _trip_descriptor(vp.trip, match.trip, service_date, match.start_time)
            nxt = match.trip.stops[match.next_index]
            vp.current_stop_sequence = nxt.sequence
            vp.stop_id = nxt.stop_id
            vp.current_status = pb.VehiclePosition.IN_TRANSIT_TO
        else:
            stats["unmatched"].append(v.label)
            vp.trip.route_id = route_id
        vp.vehicle.id = v.vehicle_id
        vp.vehicle.label = v.label
        vp.position.latitude = v.lat
        vp.position.longitude = v.lon
        if v.bearing is not None:
            vp.position.bearing = float(v.bearing)
        if v.speed_mps is not None:
            vp.position.speed = float(v.speed_mps)
        if v.timestamp:
            vp.timestamp = int(v.timestamp)
        if not match or not match.updates:
            continue
        trips = [(match.trip, match.start_time, match.updates)]
        follow = next_block_trip(index, service_date, match.trip)
        if follow is not None and follow.transloc_route_id == match.trip.transloc_route_id:
            used = {match.trip.stops[m].route_stop_id for m, _ in match.updates}
            last = match.updates[-1][1]
            more = []
            for m, stop in enumerate(follow.stops):
                t = dict((r, now + s) for r, s in bus_etas).get(stop.route_stop_id)
                if t is None or t <= last or stop.route_stop_id in used:
                    continue
                more.append((m, int(round(t))))
                used.add(stop.route_stop_id)
                last = t
            if more:
                trips.append((follow, None, more))
        for trip, start_time, stop_updates in trips:
            ent = updates.entity.add()
            ent.id = f"trip-{trip.trip_id}-{v.vehicle_id}"
            tu = ent.trip_update
            _trip_descriptor(tu.trip, trip, service_date, start_time)
            tu.vehicle.id = v.vehicle_id
            tu.vehicle.label = v.label
            tu.timestamp = int(now)
            for m, t in stop_updates:
                stu = tu.stop_time_update.add()
                stu.stop_sequence = trip.stops[m].sequence
                stu.stop_id = trip.stops[m].stop_id
                stu.arrival.time = t
                if m > 0:
                    stu.departure.time = t + max(0, trip.stops[m].departure_s - trip.stops[m].arrival_s)
                else:
                    stu.departure.time = t
                stu.schedule_relationship = pb.TripUpdate.StopTimeUpdate.SCHEDULED
    return positions, updates, stats


def _epoch(text: Optional[str]) -> Optional[int]:
    if not text:
        return None
    try:
        return int(datetime.fromisoformat(str(text).replace("Z", "+00:00")).timestamp())
    except ValueError:
        return None


def build_alerts(index: FeedIndex, notices: Sequence[Dict[str, Any]], known_stop_ids: Iterable[str], now: float) -> Any:
    """Alerts FeedMessage from the active, public system notices. A notice for routes or stops this feed doesn't have
    (a lot shuttle, a CAT stop) is left out rather than widened to the whole agency."""
    msg = pb.FeedMessage()
    _header(msg, now)
    stops = set(known_stop_ids)
    for n in notices:
        if n.get("auth_only"):
            continue
        scope = n.get("target_scope") or "all"
        entities: List[Dict[str, str]] = []
        if scope == "stops":
            entities = [{"stop_id": str(s)} for s in n.get("target_stop_ids") or [] if str(s) in stops]
        elif scope == "route":
            routes = sorted({index.route_of[str(r)] for r in n.get("target_route_ids") or [] if str(r) in index.route_of})
            entities = [{"route_id": r} for r in routes]
        else:
            entities = [{"agency_id": index.agency_id}]
        if not entities:
            continue
        ent = msg.entity.add()
        ent.id = f"notice-{n.get('id') or len(msg.entity)}"
        alert = ent.alert
        start, end = _epoch(n.get("start_time")), _epoch(n.get("end_time"))
        if start or end:
            period = alert.active_period.add()
            if start:
                period.start = start
            if end:
                period.end = end
        for e in entities:
            ie = alert.informed_entity.add()
            for k, val in e.items():
                setattr(ie, k, val)
        alert.cause = pb.Alert.UNKNOWN_CAUSE
        alert.effect = pb.Alert.UNKNOWN_EFFECT
        alert.severity_level = _SEVERITY.get(str(n.get("severity") or "").lower(), pb.Alert.INFO)
        long_text = str(n.get("message") or "").strip()
        short_text = str(n.get("short_message") or "").strip() or long_text
        tr = alert.header_text.translation.add()
        tr.text, tr.language = short_text, "en"
        if long_text and long_text != short_text:
            tr = alert.description_text.translation.add()
            tr.text, tr.language = long_text, "en"
    return msg
