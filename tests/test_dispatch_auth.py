import importlib.util
import sys
from pathlib import Path

from fastapi.testclient import TestClient


def _load_app(monkeypatch, **env):
    for key in (
        "DISPATCH_PASS",
        "DISPATCH_PASSWORD",
        "DISPATCH_SECRET",
        "DISPATCHER_PASS",
        "DISPATCHER_PASSWORD",
        "DISPATCHER_SECRET",
        "HOWELL_PASS",
        "HOWELL_PASSWORD",
        "HOWELL_SECRET",
        "DISPATCH_PASS_FILE",
        "DISPATCH_PASSWORD_FILE",
        "DISPATCH_SECRET_FILE",
        "DISPATCHER_PASS_FILE",
        "DISPATCHER_PASSWORD_FILE",
        "DISPATCHER_SECRET_FILE",
        "HOWELL_PASS_FILE",
        "HOWELL_PASSWORD_FILE",
        "HOWELL_SECRET_FILE",
    ):
        monkeypatch.delenv(key, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    sys.modules.pop("app", None)
    spec = importlib.util.spec_from_file_location("app", Path(__file__).resolve().parents[1] / "app.py")
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules["app"] = module
    spec.loader.exec_module(module)
    return module


def test_dispatcher_and_howell_passwords(monkeypatch):
    app_module = _load_app(
        monkeypatch,
        DISPATCHER_PASS="alpha",
        HOWELL_PASS="beta",
    )
    client = TestClient(app_module.app)

    dispatcher_resp = client.post("/api/dispatcher/auth", json={"password": "alpha"})
    assert dispatcher_resp.status_code == 200
    assert dispatcher_resp.json()["secret"] == "DISPATCHER"

    howell_resp = client.post("/api/dispatcher/auth", json={"password": "beta"})
    assert howell_resp.status_code == 200
    assert howell_resp.json()["secret"] == "HOWELL"


def test_howell_password_file(monkeypatch, tmp_path):
    secret_path = tmp_path / "howell.secret"
    secret_path.write_text("gamma\n", encoding="utf-8")

    app_module = _load_app(monkeypatch, HOWELL_PASS_FILE=str(secret_path))
    client = TestClient(app_module.app)

    howell_resp = client.post("/api/dispatcher/auth", json={"password": "gamma"})
    assert howell_resp.status_code == 200
    assert howell_resp.json()["secret"] == "HOWELL"
