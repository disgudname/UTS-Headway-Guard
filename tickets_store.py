import asyncio
import json
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


@dataclass
class Ticket:
    id: str
    vehicle_label: str
    vehicle_type: Optional[str] = None
    reported_at: Optional[str] = None
    reported_by: Optional[str] = None
    ops_status: Optional[str] = None
    ops_description: Optional[str] = None
    shop_status: Optional[str] = None
    mechanic: Optional[str] = None
    diagnosis_text: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clean_field(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    return value


class TicketStore:
    def __init__(self, path: Path):
        self._path = path
        self._lock = asyncio.Lock()
        self._tickets: Dict[str, Ticket] = {}
        self._soft_purges: List[Dict[str, Any]] = []
        self._load_sync()

    def _load_sync(self) -> None:
        self._tickets.clear()
        self._soft_purges = []
        if not self._path.exists():
            self._path.parent.mkdir(parents=True, exist_ok=True)
            return
        try:
            raw = json.loads(self._path.read_text())
        except json.JSONDecodeError:
            return
        if isinstance(raw, dict):
            entries = raw.get("tickets", [])
            purges = raw.get("soft_purges", [])
        else:
            entries = raw if isinstance(raw, list) else []
            purges = []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            ticket_id = entry.get("id")
            vehicle_label = entry.get("vehicle_label")
            if not ticket_id or not vehicle_label:
                continue
            data = {**entry}
            self._tickets[ticket_id] = Ticket(
                id=ticket_id,
                vehicle_label=vehicle_label,
                vehicle_type=data.get("vehicle_type"),
                reported_at=data.get("reported_at"),
                reported_by=data.get("reported_by"),
                ops_status=data.get("ops_status"),
                ops_description=data.get("ops_description"),
                shop_status=data.get("shop_status"),
                mechanic=data.get("mechanic"),
                diagnosis_text=data.get("diagnosis_text"),
                started_at=data.get("started_at"),
                completed_at=data.get("completed_at"),
                created_at=data.get("created_at", _now_iso()),
                updated_at=data.get("updated_at", _now_iso()),
            )
        for purge in purges:
            record = _normalize_purge_record(purge)
            if record is not None:
                self._soft_purges.append(record)

    async def _persist(self) -> None:
        data = {
            "tickets": [ticket.to_dict() for ticket in self._tickets.values()],
            "soft_purges": self._soft_purges,
        }
        tmp_path = self._path.with_suffix(self._path.suffix + ".tmp")
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path.write_text(json.dumps(data, indent=2, sort_keys=True))
        tmp_path.replace(self._path)

    async def list_tickets(self, include_closed: bool) -> List[Dict[str, Any]]:
        async with self._lock:
            items = list(self._tickets.values())
        filtered: List[Ticket] = []
        for ticket in items:
            if not include_closed and ticket.completed_at:
                continue
            if self._is_soft_purged(ticket):
                continue
            filtered.append(ticket)
        filtered.sort(key=lambda t: _sort_key(t.reported_at, t.created_at), reverse=True)
        return [ticket.to_dict() for ticket in filtered]

    async def get_ticket(self, ticket_id: Any) -> Optional[Dict[str, Any]]:
        async with self._lock:
            lookup_id = str(ticket_id)
            ticket = self._tickets.get(lookup_id)
            if not ticket:
                return None
            return ticket.to_dict()

    async def create_ticket(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        vehicle_label = _clean_field(payload.get("fleet_no"))
        if not vehicle_label:
            vehicle_label = _clean_field(payload.get("vehicle_name"))
        if not vehicle_label:
            raise ValueError("vehicle identifier required")

        async with self._lock:
            requested_id = payload.get("ticket_id") or payload.get("id")
            if requested_id:
                ticket_id = str(requested_id)
            else:
                ticket_id = str(uuid.uuid4())

            existing = self._tickets.get(ticket_id)
            if existing:
                return existing.to_dict()

            now = _now_iso()
            ticket = Ticket(
                id=ticket_id,
                vehicle_label=vehicle_label,
                vehicle_type=_clean_field(payload.get("vehicle_type")),
                reported_at=_clean_field(payload.get("reported_at")),
                reported_by=_clean_field(payload.get("reported_by")),
                ops_status=_clean_field(payload.get("ops_status")),
                ops_description=_clean_field(payload.get("ops_description")),
                shop_status=_clean_field(payload.get("shop_status")),
                mechanic=_clean_field(payload.get("mechanic")),
                diagnosis_text=_clean_field(payload.get("diagnosis_text")),
                started_at=_clean_field(payload.get("started_at")),
                completed_at=_clean_field(payload.get("completed_at")),
                created_at=now,
                updated_at=now,
            )
            self._tickets[ticket_id] = ticket
            await self._persist()
        return ticket.to_dict()

    async def update_ticket(self, ticket_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        async with self._lock:
            ticket = self._tickets.get(ticket_id)
            if not ticket:
                raise KeyError(ticket_id)
            updated = False
            for key in (
                "reported_at",
                "reported_by",
                "ops_status",
                "ops_description",
                "shop_status",
                "mechanic",
                "diagnosis_text",
                "started_at",
                "completed_at",
            ):
                if key in payload:
                    value = _clean_field(payload.get(key))
                    setattr(ticket, key, value)
                    updated = True
            if updated:
                ticket.updated_at = _now_iso()
                await self._persist()
            return ticket.to_dict()

    async def export_tickets(
        self,
        start: Any,
        end: Any,
        date_field: str,
        include_closed: bool,
    ) -> List[Dict[str, Any]]:
        start_dt = _parse_iso_datetime(start)
        end_dt = _parse_iso_datetime(end)
        if not start_dt or not end_dt:
            raise ValueError("invalid start or end")
        if start_dt > end_dt:
            raise ValueError("start must be before end")
        allowed_fields = {"reported_at", "started_at", "completed_at", "updated_at"}
        if date_field not in allowed_fields:
            raise ValueError("invalid dateField")
        async with self._lock:
            tickets = list(self._tickets.values())
            purges_snapshot = list(self._soft_purges)
        start_ts = start_dt.timestamp()
        end_ts = end_dt.timestamp()
        selected: List[Tuple[Ticket, float]] = []
        for ticket in tickets:
            if not include_closed and ticket.completed_at:
                continue
            if _is_soft_purged_by_records(ticket, purges_snapshot):
                continue
            field_value = getattr(ticket, date_field, None)
            if not field_value:
                continue
            field_dt = _parse_iso_datetime(field_value)
            if not field_dt:
                continue
            field_ts = field_dt.timestamp()
            if field_ts < start_ts or field_ts > end_ts:
                continue
            selected.append((ticket, field_ts))
        selected.sort(
            key=lambda item: (
                item[0].vehicle_label or "",
                item[1],
                item[0].id,
            )
        )
        return [ticket.to_dict() for ticket, _ in selected]

    async def purge_tickets(
        self,
        start: Any,
        end: Any,
        date_field: str,
        vehicles: Sequence[Any],
        hard: bool,
    ) -> Dict[str, Any]:
        start_dt = _parse_iso_datetime(start)
        end_dt = _parse_iso_datetime(end)
        if not start_dt or not end_dt:
            raise ValueError("invalid start or end")
        if start_dt > end_dt:
            raise ValueError("start must be before end")
        allowed_fields = {"reported_at", "started_at", "completed_at", "updated_at"}
        if date_field not in allowed_fields:
            raise ValueError("invalid dateField")
        normalized_vehicles = _normalize_vehicle_list(vehicles)
        vehicle_set = set(normalized_vehicles)
        purge_id = str(uuid.uuid4())
        purged_ids: List[str] = []
        async with self._lock:
            for ticket_id, ticket in list(self._tickets.items()):
                if _matches_purge(ticket, date_field, start_dt, end_dt, vehicle_set):
                    purged_ids.append(ticket_id)
            persist_needed = False
            if hard:
                for ticket_id in purged_ids:
                    if self._tickets.pop(ticket_id, None) is not None:
                        persist_needed = True
            else:
                record = {
                    "purge_id": purge_id,
                    "start": start_dt.isoformat(),
                    "end": end_dt.isoformat(),
                    "date_field": date_field,
                    "vehicles": normalized_vehicles,
                    "mode": "soft",
                    "created_at": _now_iso(),
                }
                self._soft_purges.append(record)
                persist_needed = True
            if persist_needed:
                await self._persist()
        mode = "hard" if hard else "soft"
        return {"purge_id": purge_id, "purged_count": len(purged_ids), "mode": mode}

    def _is_soft_purged(self, ticket: Ticket) -> bool:
        return _is_soft_purged_by_records(ticket, self._soft_purges)


def _sort_key(primary: Optional[str], secondary: Optional[str]) -> float:
    for value in (primary, secondary):
        if not value:
            continue
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
        except ValueError:
            continue
    return 0.0


def _parse_iso_datetime(value: Any) -> Optional[datetime]:
    if not value:
        return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _normalize_vehicle_list(values: Sequence[Any]) -> List[str]:
    normalized: List[str] = []
    seen: set[str] = set()
    for value in values or []:
        cleaned = _clean_field(value)
        if cleaned is None:
            continue
        text = str(cleaned)
        if text in seen:
            continue
        normalized.append(text)
        seen.add(text)
    return normalized


def _normalize_purge_record(raw: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(raw, dict):
        return None
    start = _parse_iso_datetime(raw.get("start"))
    end = _parse_iso_datetime(raw.get("end"))
    if not start or not end:
        return None
    date_field = raw.get("date_field") or raw.get("dateField") or "reported_at"
    vehicles_raw: Iterable[Any] = raw.get("vehicles") or []
    record = {
        "purge_id": str(raw.get("purge_id") or uuid.uuid4()),
        "start": start.isoformat(),
        "end": end.isoformat(),
        "date_field": date_field if date_field in {"reported_at", "started_at", "completed_at", "updated_at"} else "reported_at",
        "vehicles": _normalize_vehicle_list(list(vehicles_raw)),
        "mode": "soft",
        "created_at": raw.get("created_at", _now_iso()),
    }
    return record


def _is_soft_purged_by_records(ticket: Ticket, records: Iterable[Dict[str, Any]]) -> bool:
    records_list = list(records or [])
    if not records_list:
        return False
    for record in records_list:
        start_dt = _parse_iso_datetime(record.get("start"))
        end_dt = _parse_iso_datetime(record.get("end"))
        if not start_dt or not end_dt:
            continue
        date_field = record.get("date_field", "reported_at")
        vehicles = record.get("vehicles") or []
        if _matches_purge(ticket, date_field, start_dt, end_dt, set(vehicles)):
            return True
    return False


def _matches_purge(
    ticket: Ticket,
    date_field: str,
    start: datetime,
    end: datetime,
    vehicles: set[str],
) -> bool:
    if vehicles and ticket.vehicle_label not in vehicles:
        return False
    value = getattr(ticket, date_field, None)
    if not value and date_field == "updated_at":
        value = ticket.updated_at
    ts = _parse_iso_datetime(value)
    if not ts:
        return False
    return start <= ts <= end


__all__ = ["TicketStore"]
