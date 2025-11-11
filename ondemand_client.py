"""Async client for TransLoc OnDemand vehicle positions."""
from __future__ import annotations

import os
import re
from typing import Optional, List

import httpx


class OnDemandClient:
    """Minimal client to authenticate with TransLoc OnDemand and fetch positions."""

    def __init__(
        self,
        login_url: str,
        positions_url: str,
        user: str,
        passwd: str,
    ) -> None:
        self._login_url = login_url
        self._positions_url = positions_url
        self._user = user
        self._passwd = passwd
        self._client: Optional[httpx.AsyncClient] = None
        self._token: Optional[str] = None

    @classmethod
    def from_env(cls) -> "OnDemandClient":
        """Build an ``OnDemandClient`` using environment configuration.

        Required environment variables (e.g. configured via Fly.io secrets):
        * ``ONDEMAND_LOGIN_URL`` - Example: ``https://login.transloc.com/login/``
        * ``ONDEMAND_POSITIONS_URL`` - Example: ``https://api.transloc.com/v1/ondemand/uva/vehicles/positions``
        * ``ONDEMAND_USER`` - TransLoc account username/email.
        * ``ONDEMAND_PASSWD`` - TransLoc account password.
        """

        login_url = (os.getenv("ONDEMAND_LOGIN_URL") or "").strip()
        positions_url = (os.getenv("ONDEMAND_POSITIONS_URL") or "").strip()
        user = (os.getenv("ONDEMAND_USER") or "").strip()
        passwd = (os.getenv("ONDEMAND_PASSWD") or "").strip()

        missing: List[str] = []
        if not login_url:
            missing.append("ONDEMAND_LOGIN_URL")
        if not positions_url:
            missing.append("ONDEMAND_POSITIONS_URL")
        if not user:
            missing.append("ONDEMAND_USER")
        if not passwd:
            missing.append("ONDEMAND_PASSWD")
        if missing:
            raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

        return cls(login_url=login_url, positions_url=positions_url, user=user, passwd=passwd)

    async def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=10.0)
        return self._client

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None
        self._token = None

    def _extract_csrf_token(self, html: str) -> str:
        match = re.search(r'name="csrf_token" value="([^"]+)"', html)
        if not match:
            raise RuntimeError("Could not find csrf_token in login page")
        return match.group(1)

    async def _login(self, force: bool = False) -> None:
        if not force and self._token:
            return

        self._token = None
        client = await self._ensure_client()

        login_page = await client.get(self._login_url, follow_redirects=True)
        login_page.raise_for_status()
        csrf_token = self._extract_csrf_token(login_page.text)

        payload = {
            "csrf_token": csrf_token,
            "username": self._user,
            "password": self._passwd,
        }

        response = await client.post(
            self._login_url,
            data=payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            follow_redirects=True,
        )
        response.raise_for_status()

        token = client.cookies.get("transloc_authn_cookie")
        if not token:
            raise RuntimeError("Login succeeded but transloc_authn_cookie not found")
        self._token = token

    async def _ensure_token(self) -> None:
        if not self._token:
            await self._login(force=True)

    async def get_vehicle_positions(self) -> list:
        await self._ensure_token()
        client = await self._ensure_client()

        headers = {
            "Authorization": f"Token {self._token}",
            "Accept": "application/json",
        }

        response = await client.get(self._positions_url, headers=headers)

        if response.status_code in {401, 403}:
            self._token = None
            await self._login(force=True)
            headers["Authorization"] = f"Token {self._token}"
            response = await client.get(self._positions_url, headers=headers)
            if response.status_code in {401, 403}:
                response.raise_for_status()

        response.raise_for_status()
        return response.json()
