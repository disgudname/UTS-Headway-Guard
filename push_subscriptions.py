"""Push subscription storage for Web Push notifications."""

import asyncio
import json
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class PushSubscription:
    """A Web Push subscription."""
    endpoint: str
    p256dh: str
    auth: str
    created_at: str
    user_agent: Optional[str] = None

    def to_subscription_info(self) -> dict:
        """Return dict in the format expected by pywebpush."""
        return {
            "endpoint": self.endpoint,
            "keys": {
                "p256dh": self.p256dh,
                "auth": self.auth,
            },
        }


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class PushSubscriptionStore:
    """File-based storage for push subscriptions, keyed by endpoint."""

    def __init__(self, path: Path):
        self._path = path
        self._lock = asyncio.Lock()
        self._subscriptions: Dict[str, PushSubscription] = {}
        self._load_sync()

    def _load_sync(self) -> None:
        self._subscriptions.clear()
        if not self._path.exists():
            self._path.parent.mkdir(parents=True, exist_ok=True)
            return
        try:
            raw = json.loads(self._path.read_text())
        except (json.JSONDecodeError, OSError):
            return
        entries = raw.get("subscriptions", []) if isinstance(raw, dict) else []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            endpoint = entry.get("endpoint")
            p256dh = entry.get("p256dh")
            auth = entry.get("auth")
            if not endpoint or not p256dh or not auth:
                continue
            self._subscriptions[endpoint] = PushSubscription(
                endpoint=endpoint,
                p256dh=p256dh,
                auth=auth,
                created_at=entry.get("created_at", _now_iso()),
                user_agent=entry.get("user_agent"),
            )

    def _serialise_state(self) -> str:
        data = {
            "subscriptions": [asdict(sub) for sub in self._subscriptions.values()],
            "updated_at": _now_iso(),
        }
        return json.dumps(data, indent=2, sort_keys=True)

    async def _persist(self) -> None:
        payload = self._serialise_state()
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self._path.with_suffix(self._path.suffix + ".tmp")
        tmp_path.write_text(payload)
        tmp_path.replace(self._path)

    async def add_subscription(
        self,
        endpoint: str,
        keys: dict,
        user_agent: Optional[str] = None,
    ) -> bool:
        """Add or update a subscription. Returns True if new."""
        p256dh = keys.get("p256dh", "")
        auth = keys.get("auth", "")
        if not endpoint or not p256dh or not auth:
            return False

        async with self._lock:
            is_new = endpoint not in self._subscriptions
            self._subscriptions[endpoint] = PushSubscription(
                endpoint=endpoint,
                p256dh=p256dh,
                auth=auth,
                created_at=_now_iso(),
                user_agent=user_agent,
            )
            await self._persist()
            return is_new

    async def remove_subscription(self, endpoint: str) -> bool:
        """Remove a subscription by endpoint. Returns True if found."""
        async with self._lock:
            if endpoint not in self._subscriptions:
                return False
            del self._subscriptions[endpoint]
            await self._persist()
            return True

    async def get_all_subscriptions(self) -> List[PushSubscription]:
        """Return all active subscriptions."""
        async with self._lock:
            return list(self._subscriptions.values())

    async def count(self) -> int:
        """Return count of subscriptions."""
        async with self._lock:
            return len(self._subscriptions)


__all__ = ["PushSubscription", "PushSubscriptionStore"]
