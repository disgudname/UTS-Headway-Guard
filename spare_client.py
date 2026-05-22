"""Async Spare Labs paratransit API client."""
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional
import httpx

SPARE_US_BASE = "https://api.us.sparelabs.com/v1"
SPARE_US2_BASE = "https://api.us2.sparelabs.com/v1"


class SpareClient:
    """Minimal async Spare API client using Bearer token authentication."""

    def __init__(self, api_key: str, base_url: str = SPARE_US_BASE) -> None:
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._client: Optional[httpx.AsyncClient] = None

    @classmethod
    def from_env(cls) -> "SpareClient":
        api_key = (os.getenv("SPARE_API_KEY") or "").strip()
        if not api_key:
            raise RuntimeError("Missing required env var: SPARE_API_KEY")
        base_url = (os.getenv("SPARE_BASE_URL") or SPARE_US_BASE).strip() or SPARE_US_BASE
        return cls(api_key=api_key, base_url=base_url)

    async def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=15.0)
        return self._client

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    def _auth_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._api_key}",
            "Accept": "application/json",
        }

    async def get(self, path: str, **params: Any) -> Any:
        client = await self._ensure_client()
        url = f"{self._base_url}/{path.lstrip('/')}"
        filtered = {k: v for k, v in params.items() if v is not None}
        resp = await client.get(url, params=filtered, headers=self._auth_headers())
        resp.raise_for_status()
        return resp.json()

    async def get_requests(self, **params: Any) -> List[Dict[str, Any]]:
        data = await self.get("requests", **params)
        if isinstance(data, list):
            return data
        return data.get("data", [])

    async def get_duty_schedules(self, **params: Any) -> List[Dict[str, Any]]:
        data = await self.get("dutySchedules/duties", **params)
        if isinstance(data, list):
            return data
        return data.get("data", [])

    async def get_vehicles(self) -> List[Dict[str, Any]]:
        data = await self.get("vehicles")
        if isinstance(data, list):
            return data
        return data.get("data", [])

    async def get_slots(self, **params: Any) -> List[Dict[str, Any]]:
        data = await self.get("slots", **params)
        if isinstance(data, list):
            return data
        return data.get("data", [])

    async def register_webhook(
        self,
        url: str,
        types: List[str],
        secret_header: Optional[str] = None,
        secret_value: Optional[str] = None,
    ) -> Dict[str, Any]:
        client = await self._ensure_client()
        body: Dict[str, Any] = {"url": url, "types": types}
        if secret_header and secret_value:
            body["headers"] = [{"name": secret_header, "value": secret_value}]
        resp = await client.post(
            f"{self._base_url}/webhooks",
            json=body,
            headers=self._auth_headers(),
        )
        resp.raise_for_status()
        return resp.json()

    async def list_webhooks(self) -> List[Dict[str, Any]]:
        data = await self.get("webhooks")
        if isinstance(data, list):
            return data
        return data.get("data", [])

    async def delete_webhook(self, webhook_id: str) -> None:
        client = await self._ensure_client()
        resp = await client.delete(
            f"{self._base_url}/webhooks/{webhook_id}",
            headers=self._auth_headers(),
        )
        resp.raise_for_status()
