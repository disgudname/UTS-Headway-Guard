import importlib
import os
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Optional

from fastapi.testclient import TestClient


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


DISPATCH_PREFIXES = ("DISPATCH", "HOWELL")


@contextmanager
def dispatch_env(**env_vars: str):
    """Temporarily override dispatcher-related environment variables."""

    preserved: dict[str, str] = {}
    # Remove any existing dispatcher-related variables so they don't bleed into tests.
    for key in list(os.environ):
        if key.upper().startswith(DISPATCH_PREFIXES):
            preserved[key] = os.environ.pop(key)

    original_values: dict[str, Optional[str]] = {
        key: os.environ.get(key) for key in env_vars
    }
    try:
        for key, value in env_vars.items():
            os.environ[key] = value
        yield
    finally:
        for key, original in original_values.items():
            if original is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = original
        for key, value in preserved.items():
            os.environ[key] = value


def reload_app():
    if "app" in sys.modules:
        del sys.modules["app"]
    return importlib.import_module("app")


def auth_status(client):
    return client.post("/api/dispatcher/auth", json={"password": "starfleet"})


def test_howell_password_allows_mixed_case_env_key():
    with dispatch_env(Howell_Pass="starfleet"):
        app_module = reload_app()
        client = TestClient(app_module.app)
        response = auth_status(client)
        assert response.status_code == 200
        assert response.json().get("secret") == "HOWELL"


def test_howell_password_still_rejects_wrong_secret():
    with dispatch_env(Howell_Pass="starfleet"):
        app_module = reload_app()
        client = TestClient(app_module.app)
        wrong = client.post("/api/dispatcher/auth", json={"password": "wrong"})
        assert wrong.status_code == 401
        assert wrong.json().get("detail") == "Incorrect password."


def test_howell_password_allows_unicode_secret():
    with dispatch_env(HOWELL_PASS="päss"):
        app_module = reload_app()
        client = TestClient(app_module.app)
        response = client.post("/api/dispatcher/auth", json={"password": "päss"})
        assert response.status_code == 200
        assert response.json().get("secret") == "HOWELL"
