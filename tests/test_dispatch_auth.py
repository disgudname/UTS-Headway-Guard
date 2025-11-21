import os
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest.mock import patch

from fastapi.testclient import TestClient

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import httpx

from app import app, _refresh_dispatch_passwords, ondemand_positions_cache  # noqa: E402


@contextmanager
def dispatch_passwords(*, cat_passwords: Optional[Dict[str, str]] = None, **passwords: str):
    env_updates = {f"{label.upper()}_PASS": secret for label, secret in passwords.items()}
    if cat_passwords:
        env_updates.update(
            {f"{label.upper()}_CAT_PASS": secret for label, secret in cat_passwords.items()}
        )
    with patch.dict(os.environ, env_updates, clear=False):
        _refresh_dispatch_passwords(force=True)
        try:
            yield
        finally:
            _refresh_dispatch_passwords(force=True)


class DummyOnDemandClient:
    def __init__(self, roster: List[Dict[str, Any]], positions: List[Dict[str, Any]]):
        self._roster = roster
        self._positions = positions

    async def get_vehicle_details(self):
        return self._roster

    async def get_vehicle_positions(self):
        return self._positions

    async def get_resource(self, url: str, extra_headers=None):  # noqa: ARG002
        return httpx.Response(200, json=[])


def _reset_ondemand_cache() -> None:
    ondemand_positions_cache.value = None
    ondemand_positions_cache.ts = 0.0


def test_accepts_known_password():
    with dispatch_passwords(dispatch="dispatch-secret"):
        client = TestClient(app)
        response = client.post("/api/dispatcher/auth", json={"password": "dispatch-secret"})
        assert response.status_code == 200
        payload = response.json()
        assert payload["ok"] is True
        assert payload["secret"] == "dispatch"
        assert payload["access_type"] == "uts"


def test_rejects_unknown_password():
    with dispatch_passwords(dispatch="dispatch-secret"):
        client = TestClient(app)
        response = client.post("/api/dispatcher/auth", json={"password": "wrong"})
        assert response.status_code == 401
        assert response.json().get("detail") == "Incorrect password."


def test_status_reports_authorized_with_valid_cookie():
    with dispatch_passwords(alpha="alpha-secret", beta="beta-secret"):
        client = TestClient(app)
        login = client.post("/api/dispatcher/auth", json={"password": "beta-secret"})
        assert login.status_code == 200

        status = client.get("/api/dispatcher/auth")
        assert status.status_code == 200
        payload = status.json()
        assert payload["required"] is True
        assert payload["authorized"] is True
        assert payload["secret"] == "beta"
        assert payload["access_type"] == "uts"


def test_invalid_cookie_is_rejected():
    client = TestClient(app)
    client.cookies.set("dispatcher_auth", "beta:tampered")
    status = client.get("/api/dispatcher/auth")
    assert status.status_code == 200
    payload = status.json()
    assert payload["authorized"] is False
    assert payload["secret"] is None
    assert payload["access_type"] is None


def test_cat_password_sets_cat_access_type():
    with dispatch_passwords(cat_passwords={"ops": "OPS_CAT_PASS"}):
        client = TestClient(app)
        response = client.post("/api/dispatcher/auth", json={"password": "OPS_CAT_PASS"})
        assert response.status_code == 200
        payload = response.json()
        assert payload["access_type"] == "cat"
        cookie_value = client.cookies.get("dispatcher_auth")
        assert cookie_value is not None and ":cat:" in cookie_value

        status = client.get("/api/dispatcher/auth")
        assert status.status_code == 200
        status_payload = status.json()
        assert status_payload["authorized"] is True
        assert status_payload["access_type"] == "cat"


def test_cat_secret_is_detected_when_key_uses_cat_suffix():
    with dispatch_passwords(cat_passwords={"ops": "regular-secret"}):
        client = TestClient(app)
        response = client.post(
            "/api/dispatcher/auth", json={"password": "regular-secret"}
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["access_type"] == "cat"
        cookie_value = client.cookies.get("dispatcher_auth")
        assert cookie_value is not None and ":cat:" in cookie_value


def test_positions_requires_authentication():
    with dispatch_passwords(dispatch="dispatch-secret"):
        client = TestClient(app)
        original_client = getattr(app.state, "ondemand_client", None)
        try:
            _reset_ondemand_cache()
            response = client.get("/api/ondemand")
            assert response.status_code == 401
        finally:
            app.state.ondemand_client = original_client
            _reset_ondemand_cache()


def test_positions_returns_data_when_authenticated():
    with dispatch_passwords(dispatch="dispatch-secret"):
        client = TestClient(app)
        original_client = getattr(app.state, "ondemand_client", None)
        dummy = DummyOnDemandClient(
            roster=[{"vehicle_id": "123", "color": "336699"}],
            positions=[{"vehicle_id": "123", "lat": 1.23, "lon": 4.56}],
        )
        try:
            app.state.ondemand_client = dummy
            _reset_ondemand_cache()

            login = client.post("/api/dispatcher/auth", json={"password": "dispatch-secret"})
            assert login.status_code == 200

            response = client.get("/api/ondemand")
            assert response.status_code == 200
            data = response.json()
            assert isinstance(data, dict)
            vehicles = data.get("vehicles")
            assert isinstance(vehicles, list)
            assert vehicles[0]["markerColor"] == "#336699"
            assert vehicles[0]["vehicleId"] == "123"
        finally:
            app.state.ondemand_client = original_client
            _reset_ondemand_cache()


def test_positions_includes_driver_name_from_positions_payload():
    with dispatch_passwords(dispatch="dispatch-secret"):
        client = TestClient(app)
        original_client = getattr(app.state, "ondemand_client", None)
        dummy = DummyOnDemandClient(
            roster=[],
            positions=[
                {
                    "vehicle_id": "999",
                    "position": {"latitude": 38.03, "longitude": -78.51},
                    "driver": {"first_name": "James", "last_name": "Thompson"},
                }
            ],
        )
        try:
            app.state.ondemand_client = dummy
            _reset_ondemand_cache()

            login = client.post("/api/dispatcher/auth", json={"password": "dispatch-secret"})
            assert login.status_code == 200

            response = client.get("/api/ondemand")
            assert response.status_code == 200
            data = response.json()
            vehicles = data.get("vehicles") if isinstance(data, dict) else []
            assert vehicles and vehicles[0].get("driverName") == "James Thompson"
        finally:
            app.state.ondemand_client = original_client
            _reset_ondemand_cache()
