"""Async client for TransLoc OnDemand vehicle positions."""
from __future__ import annotations

import os
import re
from html.parser import HTMLParser
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
        vehicles_url: Optional[str] = None,
    ) -> None:
        self._login_url = login_url
        self._positions_url = positions_url
        if vehicles_url:
            self._vehicles_url = vehicles_url
        else:
            base_url = positions_url.rstrip("/")
            if base_url.lower().endswith("/positions"):
                base_url = base_url[: -len("/positions")]
            self._vehicles_url = f"{base_url}?show_last_active_at=true"
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

        vehicles_url = (os.getenv("ONDEMAND_VEHICLES_URL") or "").strip()

        return cls(
            login_url=login_url,
            positions_url=positions_url,
            user=user,
            passwd=passwd,
            vehicles_url=vehicles_url or None,
        )

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
        """Extract a CSRF token from the login page HTML."""

        class _CSRFParser(HTMLParser):
            def __init__(self) -> None:
                super().__init__()
                self.token: Optional[str] = None

            def handle_starttag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None:
                if self.token is not None or tag.lower() not in {"input", "meta"}:
                    return

                attr_dict = {key.lower(): value for key, value in attrs}

                # Handle hidden input fields
                name = attr_dict.get("name")
                if name and name.lower() in {"csrf_token", "csrfmiddlewaretoken"}:
                    value = attr_dict.get("value")
                    if value:
                        self.token = value
                        return

                # Handle meta tag variant (e.g. <meta name="csrf-token" content="...")
                if tag.lower() == "meta":
                    meta_name = attr_dict.get("name")
                    if meta_name and meta_name.lower() in {"csrf-token", "csrf_token"}:
                        content = attr_dict.get("content")
                        if content:
                            self.token = content

        parser = _CSRFParser()
        parser.feed(html)

        if parser.token:
            return parser.token

        # Fallback to regex in case attributes are ordered unusually.
        match = re.search(r"csrf[-_]?token\s*['\"]?[:=]['\"]([^'\"]+)", html, flags=re.IGNORECASE)
        if match:
            return match.group(1)

        raise RuntimeError("Could not find csrf_token in login page")

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

    async def get_vehicle_details(self) -> list:
        await self._ensure_token()
        client = await self._ensure_client()

        headers = {
            "Authorization": f"Token {self._token}",
            "Accept": "application/json",
        }

        response = await client.get(self._vehicles_url, headers=headers)

        if response.status_code in {401, 403}:
            self._token = None
            await self._login(force=True)
            headers["Authorization"] = f"Token {self._token}"
            response = await client.get(self._vehicles_url, headers=headers)
            if response.status_code in {401, 403}:
                response.raise_for_status()

        response.raise_for_status()
        data = response.json()
        if isinstance(data, list):
            return data
        return []
