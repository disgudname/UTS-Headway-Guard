import sys
from pathlib import Path

from fastapi.testclient import TestClient

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app import app  # noqa: E402


def test_accepts_any_password_with_pass_suffix():
    client = TestClient(app)
    response = client.post("/api/dispatcher/auth", json={"password": "alpha_PASS"})
    assert response.status_code == 200
    assert response.json() == {"ok": True, "secret": "alpha"}


def test_rejects_password_without_suffix():
    client = TestClient(app)
    response = client.post("/api/dispatcher/auth", json={"password": "alpha"})
    assert response.status_code == 401
    assert response.json().get("detail") == "Incorrect password."


def test_status_reports_authorized_with_valid_cookie():
    client = TestClient(app)
    login = client.post("/api/dispatcher/auth", json={"password": "beta_PASS"})
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
