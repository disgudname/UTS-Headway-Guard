import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import app as app_module  # noqa: E402
from tickets_store import TicketStore  # noqa: E402


@pytest.fixture()
def ticket_client(tmp_path, monkeypatch):
    data_path = tmp_path / "tickets.json"
    store = TicketStore(data_path)
    monkeypatch.setattr(app_module, "tickets_store", store, raising=False)
    client = TestClient(app_module.app)
    try:
        yield client, store
    finally:
        client.close()


def test_hard_purge_removes_tickets(ticket_client):
    client, store = ticket_client
    ticket_payload = {
        "fleet_no": "25131",
        "reported_at": "2024-01-01T12:00:00Z",
        "ops_status": "open",
    }
    create = client.post("/api/tickets", json=ticket_payload)
    assert create.status_code == 200

    response = client.post(
        "/api/tickets/purge",
        json={
            "start": "2023-12-31T00:00:00Z",
            "end": "2024-12-31T23:59:59Z",
            "dateField": "reported_at",
            "vehicles": ["25131"],
            "hard": True,
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["mode"] == "hard"
    assert payload["purged_count"] == 1

    remaining = client.get("/api/tickets")
    assert remaining.status_code == 200
    assert remaining.json() == []
    assert store._tickets == {}


def test_soft_purge_hides_tickets(ticket_client):
    client, store = ticket_client
    ticket_payload = {
        "fleet_no": "25231",
        "reported_at": "2024-02-15T09:30:00Z",
        "ops_status": "open",
    }
    create = client.post("/api/tickets", json=ticket_payload)
    assert create.status_code == 200

    response = client.post(
        "/api/tickets/purge",
        json={
            "start": "2024-02-01T00:00:00Z",
            "end": "2024-02-28T23:59:59Z",
            "dateField": "reported_at",
            "vehicles": ["25231"],
            "hard": False,
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["mode"] == "soft"
    assert payload["purged_count"] == 1

    remaining = client.get("/api/tickets")
    assert remaining.status_code == 200
    assert remaining.json() == []
    assert len(store._tickets) == 1
    assert store._soft_purges
