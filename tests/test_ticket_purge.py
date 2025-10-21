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
    assert payload["machine_id"] == "unknown"
    assert payload["mode"] == "hard"
    assert payload["purged_count"] == 1

    remaining = client.get("/api/tickets")
    assert remaining.status_code == 200
    remaining_payload = remaining.json()
    assert remaining_payload["tickets"] == []
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
    assert payload["machine_id"] == "unknown"
    assert payload["mode"] == "soft"
    assert payload["purged_count"] == 1

    remaining = client.get("/api/tickets")
    assert remaining.status_code == 200
    remaining_payload = remaining.json()
    assert remaining_payload["tickets"] == []
    assert len(store._tickets) == 1
    assert store._soft_purges


def test_complete_ticket_keeps_id_and_can_be_fetched(ticket_client):
    client, store = ticket_client
    ticket_payload = {
        "fleet_no": "25331",
        "reported_at": "2024-03-10T08:15:00Z",
        "ops_status": "open",
    }
    create = client.post("/api/tickets", json=ticket_payload)
    assert create.status_code == 200
    created_ticket = create.json()["ticket"]
    ticket_id = created_ticket["id"]

    complete = client.put(
        f"/api/tickets/{ticket_id}",
        json={"completed_at": "2024-03-11"},
    )
    assert complete.status_code == 200
    completed_ticket = complete.json()["ticket"]
    assert completed_ticket["id"] == ticket_id
    assert completed_ticket["completed_at"] == "2024-03-11"
    assert set(store._tickets.keys()) == {ticket_id}
    assert store._tickets[ticket_id].completed_at == "2024-03-11"

    fetched = client.get(f"/api/tickets/{ticket_id}")
    assert fetched.status_code == 200
    fetched_ticket = fetched.json()["ticket"]
    assert fetched_ticket["id"] == ticket_id
    assert fetched_ticket["completed_at"] == "2024-03-11"


def test_get_ticket_returns_404_for_missing(ticket_client):
    client, _ = ticket_client
    response = client.get("/api/tickets/non-existent")
    assert response.status_code == 404
    assert response.json()["detail"] == "ticket not found"
