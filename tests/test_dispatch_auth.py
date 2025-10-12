import os
import sys
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app import app, _refresh_dispatch_passwords  # noqa: E402


@contextmanager
def dispatch_passwords(**passwords: str):
    env_updates = {f"{label.upper()}_PASS": secret for label, secret in passwords.items()}
    with patch.dict(os.environ, env_updates, clear=False):
        _refresh_dispatch_passwords(force=True)
        try:
            yield
        finally:
            _refresh_dispatch_passwords(force=True)


def test_accepts_known_password():
    with dispatch_passwords(dispatch="dispatch-secret"):
        client = TestClient(app)
        response = client.post("/api/dispatcher/auth", json={"password": "dispatch-secret"})
        assert response.status_code == 200
        assert response.json() == {"ok": True, "secret": "dispatch"}


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


def test_invalid_cookie_is_rejected():
    client = TestClient(app)
    client.cookies.set("dispatcher_auth", "beta:tampered")
    status = client.get("/api/dispatcher/auth")
    assert status.status_code == 200
    payload = status.json()
    assert payload["authorized"] is False
    assert payload["secret"] is None
