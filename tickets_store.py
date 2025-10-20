import asyncio
import json
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


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
        self._load_sync()

    def _load_sync(self) -> None:
        if not self._path.exists():
            self._path.parent.mkdir(parents=True, exist_ok=True)
            return
        try:
            raw = json.loads(self._path.read_text())
        except json.JSONDecodeError:
            return
        if not isinstance(raw, list):
            return
        for entry in raw:
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

    async def _persist(self) -> None:
        data = [ticket.to_dict() for ticket in self._tickets.values()]
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
            filtered.append(ticket)
        filtered.sort(key=lambda t: _sort_key(t.reported_at, t.created_at), reverse=True)
        return [ticket.to_dict() for ticket in filtered]

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


def _sort_key(primary: Optional[str], secondary: Optional[str]) -> float:
    for value in (primary, secondary):
        if not value:
            continue
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
        except ValueError:
            continue
    return 0.0


__all__ = ["TicketStore"]
